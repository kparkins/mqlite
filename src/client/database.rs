//! Lightweight database-namespace handle.
//!
//! [`Database`] is returned by [`crate::Client::database`] and represents a
//! named namespace within the mqlite file.  It is a thin wrapper around
//! `Arc<ClientInner>` plus the database name string — all storage state lives
//! in the [`super::ClientInner`] it references.
//!
//! # Object model
//!
//! ```text
//! Client::open(path)           ← file-level entry point
//!   └─ client.database("mydb") ← this type
//!        └─ db.collection::<T>("users")  ← typed CRUD handle
//! ```
//!
//! Collection names accessed through a `Database` handle are **qualified** as
//! `<db_name>.<collection_name>` in the storage engine.  This ensures that
//! two databases with the same collection name do not share data even when
//! backed by the same file.

use serde::{de::DeserializeOwned, Serialize};
use std::{path::Path, sync::Arc};

use super::{Collection, ClientInner};
use crate::error::Result;

// ---------------------------------------------------------------------------
// Database — lightweight namespace handle
// ---------------------------------------------------------------------------

/// A handle to a named database namespace within a [`crate::Client`].
///
/// `Database` is cheap to clone — all clones share the same underlying
/// [`crate::client::ClientInner`] and therefore the same storage state.
///
/// Collection names accessed through this handle are **qualified** as
/// `<db_name>.<collection_name>` in the engine, providing logical multi-database
/// isolation within a single mqlite file.
///
/// # Example
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
/// users.insert_one(&User { name: "Alice".into() })?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<ClientInner>,
    pub(crate) db_name: String,
}

impl Database {
    // -------------------------------------------------------------------------
    // Namespace helpers
    // -------------------------------------------------------------------------

    /// The name of this database.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.db_name
    }

    /// Build the fully-qualified engine key for a collection in this database.
    ///
    /// The qualified name is `<db_name>.<collection_name>` — the same format
    /// used by the MongoDB wire protocol for namespaces.
    pub(crate) fn qualified(&self, collection_name: &str) -> String {
        format!("{}.{}", self.db_name, collection_name)
    }

    // -------------------------------------------------------------------------
    // Collection access
    // -------------------------------------------------------------------------

    /// Get a typed collection handle.
    ///
    /// This call is infallible — the collection is not created until the first write.
    /// The returned [`Collection`] uses the qualified name `<db>.<collection>` as
    /// its engine key.
    #[must_use]
    pub fn collection<T: Serialize + DeserializeOwned>(&self, name: &str) -> Collection<T> {
        Collection {
            db_name: self.db_name.clone(),
            name: name.to_owned(),
            inner: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        }
    }

    // -------------------------------------------------------------------------
    // Database-level operations
    // -------------------------------------------------------------------------

    /// List the names of all collections in this database namespace.
    ///
    /// Returns only the **unqualified** collection names (without the `db.` prefix).
    pub fn list_collection_names(&self) -> Result<Vec<String>> {
        let prefix = format!("{}.", self.db_name);
        let all = self.inner.list_collection_names()?;
        let filtered = all
            .into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(str::to_owned))
            .collect();
        Ok(filtered)
    }

    /// Drop a collection and all its indexes.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        self.inner.drop_collection(&self.qualified(name))
    }

    /// Create a collection explicitly.
    ///
    /// Collections are also created automatically on first write.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        self.inner.create_collection(&self.qualified(name))
    }

    // -------------------------------------------------------------------------
    // Persistence
    // -------------------------------------------------------------------------

    /// Force a WAL checkpoint, writing all committed data to the main file.
    ///
    /// See also [`crate::Client::checkpoint`] for a handle-based version.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the WAL, checkpoint, and close this database handle.
    ///
    /// Use this for a guaranteed-clean shutdown (e.g., before copying the file
    /// as a backup). `Drop` performs a non-blocking close.
    ///
    /// Note: this consumes the `Database` handle.  The underlying file remains
    /// open as long as any other `Client`, `Database`, or `Collection` handles
    /// that share the same `Arc<ClientInner>` are alive.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }
}
