use bson::Document;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// The result of a cursor's query plan explanation.
///
/// Returned by [`Cursor::explain`].
///
/// # Example
/// ```no_run
/// # use mqlite::{Client, doc};
/// # fn main() -> mqlite::Result<()> {
/// # let client = Client::open_in_memory()?; let db = client.database("test");
/// # let col = db.collection::<bson::Document>("orders");
/// let cursor = col.find(doc! { "status": "pending" })?;
/// let plan = cursor.explain()?;
/// println!("Plan: {}", plan.plan);
/// println!("Index used: {:?}", plan.index_used);
/// println!("Docs examined: {}", plan.docs_examined);
/// println!("Full scan: {}", plan.full_scan);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExplainResult {
    /// Human-readable description of the query plan (e.g. `"COLLSCAN"` or
    /// `"IXSCAN { email_1 }"`).
    pub plan: String,
    /// The index that was selected for this query, if any.
    ///
    /// `None` means the query engine performed a full collection scan.
    pub index_used: Option<String>,
    /// Total number of documents examined (scanned) to satisfy the query.
    ///
    /// For a full collection scan this equals the collection size at the time
    /// the cursor was created. For an index scan, it is the number of index
    /// entries visited.
    pub docs_examined: u64,
    /// Whether the query required a full collection scan (`true`) or was
    /// satisfied by an index seek (`false`).
    pub full_scan: bool,
}

/// A cursor over query results.
///
/// `Cursor<T>` implements [`Iterator`] so it can be used in `for` loops:
///
/// ```no_run
/// # use mqlite::{Client, doc};
/// # fn main() -> mqlite::Result<()> {
/// # let client = Client::open_in_memory()?; let db = client.database("test");
/// # let collection = db.collection::<bson::Document>("users");
/// let cursor = collection.find(doc! {})?;
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

#[allow(dead_code)] // Phase 0: constructors used by storage engine (Phase 1)
impl<T> Cursor<T> {
    /// Create a cursor over a pre-loaded set of documents.
    ///
    /// `docs_examined` should be the total number of documents that were
    /// scanned (not just matched) to produce the result set.
    pub(crate) fn new(docs: Vec<Document>, docs_examined: u64) -> Self {
        let plan = ExplainResult {
            plan: "COLLSCAN".to_owned(),
            index_used: None,
            docs_examined,
            full_scan: true,
        };
        Cursor {
            buffer: std::collections::VecDeque::from(docs),
            _phantom: std::marker::PhantomData,
            _not_sync: std::marker::PhantomData,
            done: false,
            plan,
        }
    }

    /// Create a cursor backed by an index scan.
    ///
    /// `docs` are the final result documents (after applying the full filter,
    /// sort, skip, limit, and projection).  `docs_examined` is the number of
    /// documents examined during the index pre-filter step — typically smaller
    /// than the total collection size.
    ///
    /// `index_name` is the name of the index that was used (e.g. `"email_1"`).
    pub(crate) fn new_index_scan(
        docs: Vec<Document>,
        docs_examined: u64,
        index_name: String,
    ) -> Self {
        let plan = ExplainResult {
            plan: format!("IXSCAN {{ {} }}", index_name),
            index_used: Some(index_name),
            docs_examined,
            full_scan: false,
        };
        Cursor {
            buffer: std::collections::VecDeque::from(docs),
            _phantom: std::marker::PhantomData,
            _not_sync: std::marker::PhantomData,
            done: false,
            plan,
        }
    }

    /// Create an empty cursor (no documents, collection was not found).
    pub(crate) fn empty() -> Self {
        let plan = ExplainResult {
            plan: "COLLSCAN".to_owned(),
            index_used: None,
            docs_examined: 0,
            full_scan: true,
        };
        Cursor {
            buffer: std::collections::VecDeque::new(),
            _phantom: std::marker::PhantomData,
            _not_sync: std::marker::PhantomData,
            done: true,
            plan,
        }
    }

    /// Returns `true` if the cursor has no remaining documents to return.
    ///
    /// A cursor becomes exhausted when the internal buffer is empty.  This is
    /// used by the wire protocol `getMore` handler to determine whether to
    /// keep the cursor in the per-connection map or remove it and return
    /// `cursor.id = 0` to the driver.
    pub fn is_exhausted(&self) -> bool {
        self.done || self.buffer.is_empty()
    }

    /// Explain the query plan that was used to produce this cursor.
    ///
    /// Returns a snapshot of the plan captured when the cursor was created.
    /// The cursor does not need to be fully consumed before calling `explain`.
    ///
    /// # Phase 1 behavior
    ///
    /// All queries in Phase 1 use a full collection scan (`COLLSCAN`).
    /// `docs_examined` reflects the collection size at cursor-creation time.
    /// Index-accelerated query plans are introduced in Phase 1c.
    ///
    /// # Example
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # fn main() -> mqlite::Result<()> {
    /// # let client = Client::open_in_memory()?; let db = client.database("test");
    /// # let col = db.collection::<bson::Document>("logs");
    /// let cursor = col.find(doc! { "level": "error" })?;
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

        match self.buffer.pop_front() {
            None => {
                self.done = true;
                None
            }
            Some(doc) => {
                let result = bson::from_document(doc).map_err(Error::BsonDeserialization);
                Some(result)
            }
        }
    }
}
