//! Fluent "action" builder types returned by [`super::Collection`] methods.
//!
//! Each builder captures a filter/update/options triple, exposes chainable
//! option setters, and executes the operation in `run()`. Fields are
//! `pub(super)` so `super::Collection` can construct them directly.

use bson::Document;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    cursor::Cursor,
    error::Result,
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, ReturnDocument, UpdateOptions,
    },
    results::{InsertManyResult, UpdateResult},
};

use super::Collection;

/// Action returned by [`Collection::find`]. Chain option methods, then call `.run()`.
pub struct Find<'a, T> {
    pub(super) coll: &'a Collection<T>,
    pub(super) filter: Document,
    pub(super) options: FindOptions,
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
    pub(super) coll: &'a Collection<T>,
    pub(super) docs: &'a [T],
    pub(super) options: InsertManyOptions,
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
    pub(super) coll: &'a Collection<T>,
    pub(super) filter: Document,
    pub(super) update: Document,
    pub(super) options: UpdateOptions,
    pub(super) multi: bool,
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
    pub(super) coll: &'a Collection<T>,
    pub(super) filter: Document,
    pub(super) update: Document,
    pub(super) options: FindOneAndUpdateOptions,
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
    pub(super) coll: &'a Collection<T>,
    pub(super) filter: Document,
    pub(super) options: FindOneAndDeleteOptions,
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
    pub(super) coll: &'a Collection<T>,
    pub(super) filter: Document,
    pub(super) replacement: &'a T,
    pub(super) options: FindOneAndReplaceOptions,
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
