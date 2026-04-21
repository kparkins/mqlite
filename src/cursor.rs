use bson::Document;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::query::explain::ExplainResult;

/// A cursor over query results.
///
/// `Cursor<T>` implements [`Iterator`] so it can be used in `for` loops:
///
/// ```no_run
/// # use mqlite::{Client, doc};
/// # use tempfile::TempDir;
/// # fn main() -> mqlite::Result<()> {
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
/// # let db = client.database("test");
/// # let collection = db.collection::<bson::Document>("users");
/// let cursor = collection.find(doc! {}).run()?;
/// for result in cursor {
///     let doc = result?;
///     println!("{:?}", doc);
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Thread Safety
///
/// `Cursor<T>` is [`Send`] — it can be moved to another thread.  It is
/// intentionally **not** [`Sync`] — it must not be shared between threads
/// concurrently.  Use a `Mutex<Cursor<T>>` if you need to drive the cursor
/// from multiple threads.
///
/// This follows the same contract as the MongoDB Rust driver's `Cursor<T>`.
pub struct Cursor<T> {
    /// Buffered documents not yet returned to the caller.
    buffer: std::collections::VecDeque<Document>,
    /// Phantom for the document type.
    _phantom: std::marker::PhantomData<T>,
    /// Makes `Cursor<T>` explicitly `!Sync`.
    ///
    /// `Cell<()>` is `Send + !Sync`.  Adding `PhantomData<Cell<()>>` causes
    /// the compiler to opt the struct out of `Sync` without affecting `Send`.
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
    /// Whether the cursor has been exhausted.
    done: bool,
    /// Query plan metadata captured at cursor creation time.
    plan: ExplainResult,
}

// Explicit `Send` impl: all contained types are `Send`.
// The `_not_sync` marker does not affect `Send`.
// SAFETY: Cursor holds only `VecDeque<Document>` (Send) and primitive fields.
unsafe impl<T: Send> Send for Cursor<T> {}

// `Sync` is deliberately NOT implemented.  The `_not_sync: PhantomData<Cell<()>>`
// field opts out of the auto-Sync derivation, so no explicit `impl !Sync` is
// needed (negative impls require `#![feature(negative_impls)]`).

impl<T> Cursor<T> {
    /// Create a cursor over a pre-loaded set of documents.
    ///
    /// `plan` is the [`ExplainResult`] produced by the engine's executor —
    /// returned verbatim by [`Cursor::explain`].
    pub(crate) fn new(docs: Vec<Document>, plan: ExplainResult) -> Self {
        Cursor {
            buffer: std::collections::VecDeque::from(docs),
            _phantom: std::marker::PhantomData,
            _not_sync: std::marker::PhantomData,
            done: false,
            plan,
        }
    }

    /// Returns `true` if the cursor has no remaining documents to return.
    ///
    /// A cursor becomes exhausted when the internal buffer is empty.  This is
    /// used by the wire protocol `getMore` handler to determine whether to
    /// keep the cursor in the per-connection map or remove it and return
    /// `cursor.id = 0` to the driver.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.done || self.buffer.is_empty()
    }

    /// Explain the query plan that was used to produce this cursor.
    ///
    /// Returns a snapshot of the plan captured when the cursor was created.
    /// The cursor does not need to be fully consumed before calling `explain`.
    ///
    /// # Example
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use tempfile::TempDir;
    /// # fn main() -> mqlite::Result<()> {
    /// # let dir = TempDir::new()?;
    /// # let client = Client::open(dir.path().join("db.mqlite"))?;
    /// # let db = client.database("test");
    /// # let col = db.collection::<bson::Document>("logs");
    /// let cursor = col.find(doc! { "level": "error" }).run()?;
    /// let plan = cursor.explain()?;
    /// assert_eq!(plan.full_scan, true);
    /// assert!(plan.index_used.is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(&self) -> Result<ExplainResult> {
        Ok(self.plan.clone())
    }
}

impl<T: DeserializeOwned> Iterator for Cursor<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let Some(doc) = self.buffer.pop_front() else {
            self.done = true;
            return None;
        };
        Some(bson::from_document(doc).map_err(Error::BsonDeserialization))
    }
}
