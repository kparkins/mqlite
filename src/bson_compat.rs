/// Re-exports from the `bson` crate for user convenience.
///
/// Users of mqlite do not need to add `bson` as a direct dependency.
/// Import these types directly from `mqlite::`:
///
/// ```no_run
/// use mqlite::{doc, Document, Bson, ObjectId, DateTime};
/// ```

pub use bson::{
    doc,
    Bson,
    Document,
    DateTime,
    oid::ObjectId,
};
