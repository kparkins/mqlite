//! TCP listener for the MongoDB wire protocol shim.

use crate::{database::Database, error::Result};

/// A running MongoDB wire protocol server backed by an mqlite database.
///
/// The server runs in a background tokio task and stops when this handle is dropped.
///
/// # Example
/// ```no_run
/// use mqlite::{Database, WireProtocol};
///
/// let db = Database::open_in_memory()?;
/// let server = WireProtocol::bind(&db, "127.0.0.1:27017")?;
/// // Server is running. Connect with mongosh mongodb://localhost:27017
/// drop(server); // Server stops
/// # Ok::<(), mqlite::Error>(())
/// ```
pub struct WireProtocol {
    /// Channel sender used to signal the background task to shut down.
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl WireProtocol {
    /// Start the wire protocol server on the given address.
    ///
    /// Binds a TCP listener and spawns a background tokio task to handle connections.
    /// Connections are accepted on a separate task and the server is ready immediately.
    pub fn bind(_db: &Database, _addr: &str) -> Result<WireProtocol> {
        // Phase 1 stub: wire protocol implementation is tracked in hq-6d0
        let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
        Err(crate::error::Error::Internal(
            "WireProtocol::bind: wire protocol not yet implemented (Phase 1, see hq-6d0)".into(),
        ))
    }
}
