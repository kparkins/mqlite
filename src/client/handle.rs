//! The public [`Client`] handle and its lifecycle surface.
//!
//! The heavy `Client::open*` constructors live in [`super::open`].

use std::{path::Path, sync::Arc};

use crate::error::Result;

use super::{database::Database, inner::ClientInner};

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
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint I/O fails.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    ///
    /// # Errors
    ///
    /// Returns an error if the backup cannot be written.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the journal, checkpoint, and close the client.
    ///
    /// Use this when you need a guarantee that all committed data is in the main
    /// file (e.g., before copying the file as a backup). `Drop` performs a
    /// non-blocking close.
    ///
    /// # Errors
    ///
    /// Returns an error if the final checkpoint fails.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Reset Phase 8 benchmark-only journal sync counters.
    ///
    /// This hidden hook is used by release-profile benchmark targets. It is
    /// not part of the stable application API.
    #[doc(hidden)]
    pub fn __phase8_bench_reset_sync_observations(&self) {
        crate::journal::append_sync_observations::reset();
    }

    /// Return successful journal fsync boundaries observed since reset.
    ///
    /// This hidden hook is used by release-profile benchmark targets. It is
    /// not part of the stable application API.
    #[doc(hidden)]
    #[must_use]
    pub fn __phase8_bench_journal_sync_os_boundaries(&self) -> u64 {
        crate::journal::append_sync_observations::snapshot().journal_sync_os_boundaries
    }

    // Test-only accessors (`__oracle_now`, `__published_visible_ts`,
    // `__published_catalog_gen`, `__published_sequencer_frontier`,
    // `__recovery_open_published_store_count`, `__recovered_max_commit_ts`,
    // `__read_view_registry`) live in a dedicated module:
    // `src/client/tests/hidden_accessors.rs`. Keeping them out of this file
    // makes the boundary between production API and test scaffolding
    // unambiguous.
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
