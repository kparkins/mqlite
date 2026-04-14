use bson::Document;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// A cursor over query results.
///
/// `Cursor<T>` implements [`Iterator`] so it can be used in `for` loops:
///
/// ```no_run
/// # use mqlite::{Database, doc};
/// # fn main() -> mqlite::Result<()> {
/// # let db = Database::open_in_memory()?;
/// # let collection = db.collection::<bson::Document>("users");
/// let cursor = collection.find(doc! {})?;
/// for result in cursor {
///     let doc = result?;
///     println!("{:?}", doc);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Cursor<T> {
    /// Buffered documents not yet returned to the caller.
    buffer: std::collections::VecDeque<Document>,
    /// Phantom for the document type.
    _phantom: std::marker::PhantomData<T>,
    /// Whether the cursor has been exhausted.
    done: bool,
}

#[allow(dead_code)] // Phase 0: constructors used by storage engine (Phase 1)
impl<T> Cursor<T> {
    /// Create a cursor over a pre-loaded set of documents.
    /// Used internally by the collection implementation.
    pub(crate) fn new(docs: Vec<Document>) -> Self {
        Cursor {
            buffer: std::collections::VecDeque::from(docs),
            _phantom: std::marker::PhantomData,
            done: false,
        }
    }

    /// Create an empty cursor.
    pub(crate) fn empty() -> Self {
        Cursor {
            buffer: std::collections::VecDeque::new(),
            _phantom: std::marker::PhantomData,
            done: true,
        }
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
