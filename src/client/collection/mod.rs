mod actions;

pub use actions::{
    Aggregate, Find, FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate, InsertMany, Replace,
    Update,
};

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, sync::Arc};

use crate::{
    client::ClientInner,
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, UpdateOptions,
    },
    results::{DeleteResult, InsertOneResult},
    update::UpdateModifications,
};

const DEFAULT_FIND_LIMIT: i64 = 100;

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
    pub(crate) inner: Arc<ClientInner>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Self {
            db_name: self.db_name.clone(),
            name: self.name.clone(),
            inner: Arc::clone(&self.inner),
            _phantom: PhantomData,
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
            options: FindOptions {
                limit: Some(DEFAULT_FIND_LIMIT),
                ..FindOptions::default()
            },
        }
    }

    // -------------------------------------------------------------------------
    // Aggregate
    // -------------------------------------------------------------------------

    /// Run an aggregation `pipeline` over the collection.
    ///
    /// Returns an [`Aggregate`] action; call `.run()` to execute. The result is
    /// a [`crate::Cursor`] of raw [`Document`]s regardless of `T`, because a
    /// pipeline can reshape documents (mirroring the MongoDB Rust driver's
    /// untyped `aggregate`).
    ///
    /// Supported stages: `$match`, `$sort`, `$skip`, `$limit`, `$count`,
    /// `$project`, and `$group`. A leading `$match` is index-accelerated via
    /// the same planner the find path uses; all later stages run in memory.
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("orders");
    /// let cursor = col
    ///     .aggregate(vec![
    ///         doc! { "$match": { "status": "shipped" } },
    ///         doc! { "$group": { "_id": "$region", "total": { "$sum": "$amount" } } },
    ///     ])
    ///     .run()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn aggregate(&self, pipeline: Vec<Document>) -> Aggregate<'_, T> {
        Aggregate {
            coll: self,
            pipeline,
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
    pub fn update_one(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
    ) -> Update<'_, T> {
        Update {
            coll: self,
            filter,
            update: update.into(),
            options: UpdateOptions::default(),
            multi: false,
        }
    }

    /// Update all documents matching `filter`.
    ///
    /// Returns an [`Update`] action. Chain option methods before calling `.run()`.
    /// `update` is either a classic operator/replacement [`Document`] or an
    /// aggregation pipeline (`Vec<Document>`), via [`UpdateModifications`].
    #[must_use]
    pub fn update_many(
        &self,
        filter: Document,
        update: impl Into<UpdateModifications>,
    ) -> Update<'_, T> {
        Update {
            coll: self,
            filter,
            update: update.into(),
            options: UpdateOptions::default(),
            multi: true,
        }
    }

    /// Replace at most one document matching `filter` with `replacement`.
    ///
    /// Returns a [`Replace`] action. Chain `.upsert(true)` before calling
    /// `.run()`:
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("users");
    /// col.replace_one(doc! { "email": "a@b.com" }, &doc! { "email": "a@b.com" })
    ///     .upsert(true)
    ///     .run()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The serialized `replacement` is a full document image: it must not
    /// contain top-level update operators (keys beginning with `$`). The
    /// replacement keeps the matched document's `_id` when it has none; a
    /// replacement `_id` that differs from the matched `_id` is an
    /// immutable-field error. On `.upsert(true)` with no match the inserted
    /// `_id` is the replacement `_id`, else an equality `_id` from the filter,
    /// else a generated [`bson::oid::ObjectId`].
    #[must_use]
    pub fn replace_one<'a>(&'a self, filter: Document, replacement: &'a T) -> Replace<'a, T> {
        Replace {
            coll: self,
            filter,
            replacement,
            upsert: false,
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
        update: impl Into<UpdateModifications>,
    ) -> FindOneAndUpdate<'_, T> {
        FindOneAndUpdate {
            coll: self,
            filter,
            update: update.into(),
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

    /// Return the distinct values of `field_name` across documents matching
    /// `filter`, as raw BSON regardless of `T`.
    ///
    /// `field_name` is resolved as a dotted path with MongoDB array traversal:
    /// when the resolved value is an array each element is a candidate value
    /// (the array itself is not), and path segments through arrays of documents
    /// unwrap per element. A missing field contributes nothing; an explicit
    /// `null` contributes `null`; nested arrays unwrap one level only. Values
    /// are deduplicated with cross-numeric BSON equality (`1` and `1.0` are one
    /// value) and returned in first-encountered order.
    ///
    /// # Errors
    ///
    /// Returns an error if `field_name` is empty or begins with `$`, or if the
    /// query cannot be evaluated.
    pub fn distinct(&self, field_name: &str, filter: Document) -> Result<Vec<Bson>> {
        self.inner.distinct(&self.namespace(), field_name, filter)
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
