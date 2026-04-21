//! The public [`Client`] handle and its lifecycle surface.
//!
//! The heavy `Client::open*` constructors live in [`super::open`].

use std::{path::Path, sync::Arc};

use crate::{database::Database, error::Result};

use super::inner::ClientInner;

/// A connection to an mqlite database file.
///
/// `Client` is cheaply cloneable — all clones share the same underlying storage,
/// writer lock, and buffer pool.  It is the root of the mqlite object model,
/// matching the MongoDB Rust driver hierarchy:
///
/// ```text
/// Client::open("myapp.mqlite")?
///   └─ client.database("mydb")
///        └─ db.collection::<User>("users")
///             └─ col.insert_one(...) / col.find(...) / ...
/// ```
///
/// # Opening
///
/// ```no_run
/// use mqlite::Client;
///
/// // Open (or create) a database file
/// let client = Client::open("myapp.mqlite")?;
///
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
/// # Ok::<(), mqlite::Error>(())
/// ```
///
/// # Databases and Collections
///
/// ```no_run
/// use mqlite::{Client, doc};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String }
///
/// # fn main() -> mqlite::Result<()> {
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
/// let db = client.database("myapp");
/// let users = db.collection::<User>("users");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl Client {
    /// Construct a `Client` from an already-built [`ClientInner`].
    ///
    /// Used by [`super::open`] after it has assembled the engine, buffer pool,
    /// and file lock.
    pub(super) fn from_inner(inner: Arc<ClientInner>) -> Self {
        Client { inner }
    }

    /// Get a handle to a named database.
    ///
    /// This call is infallible — the database namespace is logical only.
    /// No I/O occurs; a [`Database`] handle is returned immediately.
    ///
    /// # Example
    /// ```no_run
    /// use mqlite::Client;
    ///
    /// # fn main() -> mqlite::Result<()> {
    /// # use tempfile::TempDir;
    /// # let dir = TempDir::new()?;
    /// # let client = Client::open(dir.path().join("db.mqlite"))?;
    /// let db = client.database("myapp");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn database(&self, name: &str) -> Database {
        Database {
            inner: Arc::clone(&self.inner),
            db_name: name.to_owned(),
        }
    }

    /// Force a journal checkpoint.
    ///
    /// After this returns, the main database file is safe to copy as a backup.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the journal, checkpoint, and close the client.
    ///
    /// Use this when you need a guarantee that all committed data is in the main
    /// file (e.g., before copying the file as a backup). `Drop` performs a
    /// non-blocking close.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Test-only accessor for the MVCC `ReadViewRegistry` backing this client.
    ///
    /// Exposed for integration tests that need to register external `ReadView`s
    /// and watch them get force-expired on the engine's drop path. Returns `None`
    /// when the client has no attached buffer pool.
    #[doc(hidden)]
    pub fn __read_view_registry(&self) -> Option<Arc<crate::mvcc::ReadViewRegistry>> {
        self.inner.engine.read_view_registry()
    }
}

impl Drop for Client {
    /// Non-blocking close.
    ///
    /// Checkpoints when this is the last handle. Journal data remains on disk
    /// otherwise and will be replayed automatically on the next `Client::open`.
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.inner.checkpoint();
        }
    }
}
