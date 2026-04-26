//! TCP listener and command handler for the MongoDB wire protocol shim.
//!
//! This module provides [`WireProtocol`], a background server that handles
//! MongoDB wire protocol connections using OP_MSG framing.
//!
//! # Two-opcode handshake
//!
//! pymongo 4.x sends the *initial* `isMaster` using OP_QUERY (opcode 2004),
//! the legacy opcode, because at connection time the driver does not yet know
//! the server wire version.  The response must be OP_REPLY (opcode 1).
//!
//! After receiving `helloOk: true` in the OP_REPLY, pymongo switches all
//! subsequent commands — including `hello` topology checks and CRUD — to
//! OP_MSG (opcode 2013).
//!
//! Consequently the server handles both opcodes:
//! - OP_QUERY → OP_REPLY  (initial handshake only)
//! - OP_MSG   → OP_MSG    (all subsequent commands)

mod cursors;
mod op_query;

use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

use tokio_util::sync::CancellationToken;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use bson::{doc, oid::ObjectId, Document};

use super::protocol::{MsgHeader, OpMsg, Section, MAX_MESSAGE_SIZE};
use crate::{
    client::{Client, ClientInner},
    error::Result,
};

pub(super) use super::framing::{read_message, write_message};
use super::handlers;

pub(crate) use cursors::{cursor_sweep_task, ConnectionCursors};
use op_query::{build_op_reply, parse_op_query_body, parse_op_query_db_name};

// ---------------------------------------------------------------------------
// Legacy opcodes (not in protocol.rs — used only for handshake interop)
// ---------------------------------------------------------------------------

/// OP_QUERY — legacy opcode used by MongoDB drivers for the *initial*
/// `isMaster` / `hello` handshake before wire version is established.
const OP_QUERY: i32 = 2004;

/// How long to wait for any read on an idle connection before closing it.
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

/// Shared state for the wire protocol server.
///
/// Created once at [`WireProtocol::bind`] time and cloned (cheaply, via
/// `Arc`) into each connection task.  All fields behind `Arc` are shared
/// across every connection so counters are global to the server instance.
#[derive(Clone)]
pub(crate) struct ServerState {
    /// Time when this `WireProtocol` instance was started.
    /// Used to compute uptime in the `serverStatus` response.
    pub(crate) start_time: std::time::Instant,

    /// Monotonically increasing counter used to assign unique per-connection IDs.
    /// Starts at 1; each new connection receives the old value before increment.
    pub(crate) next_connection_id: Arc<AtomicI32>,

    /// Path to the database file.
    /// Used to locate the journal file (`<path>-journal`) for `serverStatus`.
    pub(crate) db_path: Option<std::path::PathBuf>,

    /// `topologyVersion.processId` — a random [`ObjectId`] generated once at
    /// server start and included in every `hello` / `isMaster` response.
    pub(crate) topology_process_id: ObjectId,

    /// Shared client inner state — used by CRUD command handlers.
    pub(crate) database: Arc<ClientInner>,

    /// Cancellation token used to signal all connection tasks to stop.
    pub(crate) cancel: CancellationToken,

    /// Keeps the temp directory alive for the lifetime of this state.
    /// Only populated when `ServerState` is constructed without an explicit
    /// database path (i.e., in tests via `default()` or `new()`).
    #[cfg(test)]
    pub(crate) _tempdir: Option<tempfile::TempDir>,
}

#[cfg(test)]
impl Default for ServerState {
    fn default() -> Self {
        let tempdir = tempfile::TempDir::new().expect("create tempdir for default ServerState");
        let db_path = tempdir.path().join("mqlite_test.db");
        let client = Client::open(&db_path).expect("open tempdir-backed client");
        ServerState {
            start_time: std::time::Instant::now(),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: Some(db_path.clone()),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
            cancel: CancellationToken::new(),
            _tempdir: Some(tempdir),
        }
    }
}

impl ServerState {
    /// Create state backed by a tempdir-scoped [`Client`] for use in tests.
    /// `db_path` is recorded as-is so callers can pass an explicit path or
    /// `None` when the exact path does not matter.
    #[cfg(test)]
    pub(crate) fn new(db_path: Option<std::path::PathBuf>) -> Self {
        let tempdir = tempfile::TempDir::new().expect("create tempdir for ServerState::new");
        let tmp_db_path = tempdir.path().join("mqlite_test.db");
        let client = Client::open(&tmp_db_path).expect("open tempdir-backed client");
        ServerState {
            start_time: std::time::Instant::now(),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: Some(db_path.unwrap_or(tmp_db_path)),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
            cancel: CancellationToken::new(),
            _tempdir: Some(tempdir),
        }
    }

    /// Create state backed by a real [`Client`] instance.
    ///
    /// Used by [`WireProtocol::bind`] to wire CRUD handlers to the actual client.
    fn new_with_db(client: &Client) -> Self {
        ServerState {
            start_time: std::time::Instant::now(),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: client.inner.path.clone(),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
            cancel: CancellationToken::new(),
            #[cfg(test)]
            _tempdir: None,
        }
    }

    /// Reserve and return the next connection ID (pre-increment).
    pub(crate) fn next_conn_id(&self) -> i32 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Return server uptime in whole seconds.
    pub(crate) fn uptime_secs(&self) -> i64 {
        self.start_time.elapsed().as_secs() as i64
    }

    /// Return the size of the journal file in bytes, or 0 if absent.
    pub(crate) fn journal_file_size(&self) -> u64 {
        let journal_path = match &self.db_path {
            Some(p) => {
                let mut s = p.as_os_str().to_owned();
                s.push("-journal");
                std::path::PathBuf::from(s)
            }
            None => return 0,
        };
        std::fs::metadata(&journal_path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Total number of connections that have been opened since server start.
    pub(crate) fn total_connections(&self) -> i32 {
        // next_connection_id starts at 1; subtract 1 for the count of allocated IDs.
        self.next_connection_id
            .load(Ordering::Relaxed)
            .saturating_sub(1)
    }
}

// ---------------------------------------------------------------------------
// $db routing helpers
// ---------------------------------------------------------------------------

/// Extract the database name from a command body's `$db` field.
///
/// Falls back to `"test"` when the field is absent — this matches mongosh's
/// default database (i.e., `use mydb` in mongosh sends subsequent commands
/// with `$db: "mydb"`).  Any non-empty string is accepted; there is no
/// server-side database name restriction in the multi-database wire protocol.
fn extract_db_name(body: &Document) -> String {
    body.get_str("$db")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or("test")
        .to_owned()
}

/// Fully-qualify a collection name as `<db_name>.<coll_name>` using the
/// `$db` field from the command body.
///
/// This matches the engine's internal namespace format (`Database` and
/// `Collection<T>` handles store collections as `"db.collection"`).
fn qualified_coll(body: &Document, coll_name: &str) -> String {
    format!("{}.{}", extract_db_name(body), coll_name)
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// A running MongoDB wire protocol server backed by an mqlite database.
///
/// The server runs in a background thread (with its own tokio runtime) and
/// stops when this handle is dropped.
///
/// # Example
/// ```no_run
/// use mqlite::{Client, WireProtocol};
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
///
/// let server = WireProtocol::bind(&client, "127.0.0.1:27017")?;
/// // Server is running. Connect with:
/// //   mongosh "mongodb://localhost:27017/?directConnection=true"
/// //   MongoClient("mongodb://localhost:27017/?directConnection=true")
/// drop(server); // Server stops
/// # Ok::<(), mqlite::Error>(())
/// ```
pub struct WireProtocol {
    /// Dropping this sender signals the background task to stop.
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl WireProtocol {
    /// Start the wire protocol server on the given address.
    ///
    /// Spawns a background thread running a tokio runtime.  The thread binds
    /// a TCP listener and accepts connections.  Returns once the listener is
    /// bound and ready to accept connections.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the TCP listener cannot be bound (port in use, bad
    /// address, permissions, etc.).
    pub fn bind(client: &Client, addr: &str) -> Result<WireProtocol> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Channel to report bind success/failure back to the caller synchronously.
        let (bind_tx, bind_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();

        // Capture the client reference for CRUD command handlers.
        let state = ServerState::new_with_db(client);

        let addr = addr.to_owned();

        // Security: warn when binding to all interfaces — mqlite has no
        // authentication, so 0.0.0.0 exposes the server to the entire
        // network.  Default recommended bind is 127.0.0.1 (localhost only).
        if addr.starts_with("0.0.0.0") {
            eprintln!(
                "mqlite WARNING: wire protocol server bound to {addr} — \
                 accessible from all network interfaces. \
                 mqlite has no authentication. \
                 Use 127.0.0.1 for local-only access."
            );
        }

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime creation should not fail");

            rt.block_on(async move {
                // Attempt to bind; report outcome to the caller.
                let listener = match tokio::net::TcpListener::bind(&addr).await {
                    Ok(l) => {
                        let _ = bind_tx.send(Ok(()));
                        l
                    }
                    Err(e) => {
                        let _ = bind_tx.send(Err(e.to_string()));
                        return;
                    }
                };

                // Run the accept loop until the shutdown signal arrives.
                tokio::select! {
                    _ = accept_loop(listener, state.clone()) => {}
                    _ = shutdown_rx => {
                        // Signal all connection tasks to stop, then wait up
                        // to 5 seconds for them to drain.
                        state.cancel.cancel();
                    }
                }
            });
        });

        // Block until the listener is bound (or binding fails).
        match bind_rx.recv() {
            Ok(Ok(())) => Ok(WireProtocol {
                _shutdown: shutdown_tx,
            }),
            Ok(Err(e)) => Err(crate::error::Error::Io(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("WireProtocol::bind: {}", e),
            ))),
            Err(_) => Err(crate::error::Error::Internal(
                "WireProtocol::bind: background thread exited before reporting bind status".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

/// Accept incoming connections and spawn a task for each.
///
/// Uses a [`tokio::task::JoinSet`] to track all active connection tasks.
/// Exits when the [`CancellationToken`] in `state` is cancelled or the
/// listener encounters a hard error; waits up to 5 seconds for connections
/// to finish before returning.
async fn accept_loop(listener: tokio::net::TcpListener, state: ServerState) {
    let mut join_set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _peer)) => {
                        let conn_state = state.clone();
                        join_set.spawn(handle_connection(stream, conn_state));
                    }
                    // A hard listener error causes an exit.
                    Err(_) => break,
                }
            }
            _ = state.cancel.cancelled() => break,
        }
    }
    // Drain remaining connection tasks with a 5-second grace period.
    let drain = async { while join_set.join_next().await.is_some() {} };
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), drain).await;
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Handle all wire protocol messages on a single TCP connection.
///
/// Handles both OP_QUERY (initial handshake) and OP_MSG (subsequent commands).
async fn handle_connection(mut stream: TcpStream, state: ServerState) {
    let connection_id = state.next_conn_id();
    let mut next_request_id: i32 = 1;

    // Per-connection cursor map.  Dropped automatically when this function
    // returns, releasing all cursors associated with this connection.
    let cursors = Arc::new(std::sync::Mutex::new(ConnectionCursors::new()));

    // Spawn a background task to evict idle cursors (600 s timeout, 60 s sweep).
    // The task exits when `_sweep_shutdown` is dropped (end of this function).
    let (_sweep_shutdown, sweep_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(cursor_sweep_task(Arc::clone(&cursors), sweep_rx));

    loop {
        // Read the 16-byte header to determine the opcode.
        let mut header_buf = [0u8; MsgHeader::SIZE];
        let read_header = tokio::time::timeout(IDLE_TIMEOUT, stream.read_exact(&mut header_buf));
        tokio::select! {
            result = read_header => {
                match result {
                    Ok(Ok(())) => {}
                    _ => break, // timeout or I/O error
                }
            }
            _ = state.cancel.cancelled() => break,
        }

        let declared_len =
            i32::from_le_bytes(header_buf[0..4].try_into().expect("slice is 4 bytes")) as usize;
        let opcode = i32::from_le_bytes(header_buf[12..16].try_into().expect("slice is 4 bytes"));
        let request_id = i32::from_le_bytes(header_buf[4..8].try_into().expect("slice is 4 bytes"));

        // Guard against oversized messages.
        if declared_len < MsgHeader::SIZE || declared_len > MAX_MESSAGE_SIZE {
            break;
        }

        // Read the rest of the message.
        let remainder = declared_len - MsgHeader::SIZE;
        let mut rest = vec![0u8; remainder];
        match tokio::time::timeout(IDLE_TIMEOUT, stream.read_exact(&mut rest)).await {
            Ok(Ok(())) => {}
            _ => break, // timeout or I/O error
        }

        // Reassemble the full message buffer.
        let mut full = Vec::with_capacity(declared_len);
        full.extend_from_slice(&header_buf);
        full.extend_from_slice(&rest);

        // Dispatch by opcode.
        let response_bytes = match opcode {
            OP_QUERY => {
                // Legacy OP_QUERY — initial handshake from driver.
                match dispatch_op_query(&full, next_request_id, request_id, &state, connection_id) {
                    Ok(b) => b,
                    Err(_) => break,
                }
            }
            _ if opcode == super::protocol::OP_MSG => {
                // OP_MSG — all commands after handshake.
                let msg = match OpMsg::parse(&full) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                match dispatch_op_msg(
                    &msg,
                    next_request_id,
                    request_id,
                    &state,
                    connection_id,
                    &cursors,
                ) {
                    Ok(b) => b,
                    Err(_) => break,
                }
            }
            _ => {
                // Unknown opcode — close connection.
                break;
            }
        };

        if stream.write_all(&response_bytes).await.is_err() {
            break;
        }

        next_request_id = next_request_id.wrapping_add(1);
    }
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

/// Dispatch an OP_QUERY message, returning a serialised OP_REPLY response.
fn dispatch_op_query(
    full_msg: &[u8],
    request_id: i32,
    response_to: i32,
    state: &ServerState,
    connection_id: i32,
) -> Result<Vec<u8>> {
    // OP_QUERY body starts after the 16-byte header.
    let body_buf = &full_msg[MsgHeader::SIZE..];

    // Any database is accepted; the fullCollectionName db prefix is used only
    // to identify which database the OP_QUERY targets (legacy handshake only).
    let _ = parse_op_query_db_name(body_buf); // keep fn reachable for tests

    let doc = parse_op_query_body(body_buf)?;
    let command_name =
        doc.keys()
            .next()
            .ok_or_else(|| crate::error::Error::InvalidWireMessage {
                detail: "OP_QUERY command document is empty".into(),
            })?;
    // OP_QUERY is only used for the initial handshake (hello/isMaster).
    // Create a throwaway cursor map — CRUD commands never arrive via OP_QUERY.
    let dummy_cursors = Arc::new(std::sync::Mutex::new(ConnectionCursors::new()));
    let response_body = route_command(command_name, &doc, state, connection_id, &dummy_cursors);
    build_op_reply(request_id, response_to, &response_body)
}

/// Dispatch an OP_MSG message, returning a serialised OP_MSG response.
fn dispatch_op_msg(
    msg: &OpMsg,
    request_id: i32,
    response_to: i32,
    state: &ServerState,
    connection_id: i32,
    cursors: &Arc<std::sync::Mutex<ConnectionCursors>>,
) -> Result<Vec<u8>> {
    let body = msg
        .body()
        .ok_or_else(|| crate::error::Error::InvalidWireMessage {
            detail: "command message has no Kind-0 body section".into(),
        })?;
    // Any $db value is accepted — $db is used for routing, not access control.
    // Merge Kind-1 document sequences (e.g. pymongo bulk inserts) into the
    // body so handlers always see a complete document regardless of framing.
    let merged_body = merge_doc_sequences_into_body(body, &msg.sections);
    let command_name =
        merged_body
            .keys()
            .next()
            .ok_or_else(|| crate::error::Error::InvalidWireMessage {
                detail: "command body document is empty".into(),
            })?;
    let response_body = route_command(command_name, &merged_body, state, connection_id, cursors);
    OpMsg::build_response(request_id, response_to, &response_body)
}

/// Route a command name to the appropriate handler.
///
/// Silently ignores LSID / session / cluster-time fields per the wire protocol
/// spec — these are logged at DEBUG level and never returned as errors.
fn route_command(
    command_name: &str,
    body: &Document,
    state: &ServerState,
    connection_id: i32,
    cursors: &Arc<std::sync::Mutex<ConnectionCursors>>,
) -> Document {
    // Silently log (and ignore) session/cluster fields that mqlite does not support:
    // lsid, readConcern, writeConcern, $clusterTime, txnNumber.
    #[cfg(feature = "tracing")]
    {
        for key in [
            "lsid",
            "readConcern",
            "writeConcern",
            "$clusterTime",
            "txnNumber",
        ] {
            if body.contains_key(key) {
                tracing::debug!(
                    target: "mqlite",
                    field = key,
                    "mqlite::wire::ignored_field"
                );
            }
        }
    }

    #[cfg(feature = "tracing")]
    let _cmd_start = std::time::Instant::now();

    let result = match command_name.to_ascii_lowercase().as_str() {
        "hello" | "ismaster" => handlers::handle_hello(state, connection_id),
        "ping" => handlers::handle_ping(),
        "buildinfo" => handlers::handle_build_info(),
        "serverstatus" => handlers::handle_server_status(state),
        "listdatabases" => handlers::handle_list_databases(state),
        // CRUD commands
        "insert" => handlers::handle_insert(body, state),
        "find" => handlers::handle_find(body, state, cursors),
        "update" => handlers::handle_update(body, state),
        "delete" => handlers::handle_delete(body, state),
        "findandmodify" => handlers::handle_find_and_modify(body, state),
        // Cursor management
        "getmore" => handlers::handle_get_more(body, state, cursors),
        "killcursors" => handlers::handle_kill_cursors(body, cursors),
        // Collection admin
        "create" => handlers::handle_create(body, state),
        "drop" => handlers::handle_drop(body, state),
        "listcollections" => handlers::handle_list_collections(body, state),
        // Index operations
        "createindexes" => handlers::handle_create_indexes(body, state),
        "dropindexes" => handlers::handle_drop_indexes(body, state),
        "listindexes" => handlers::handle_list_indexes(body, state),
        other => handlers::handle_unknown(other),
    };

    #[cfg(feature = "tracing")]
    {
        let duration_ms = _cmd_start.elapsed().as_millis() as u64;
        let ok = result
            .get("ok")
            .and_then(|v| v.as_f64())
            .map(|v| v >= 1.0)
            .unwrap_or(false);
        tracing::debug!(
            target: "mqlite",
            command_name,
            duration_ms,
            ok,
            "mqlite::wire::command"
        );
    }

    let _ = body;

    result
}

// ---------------------------------------------------------------------------
// Helper utilities for CRUD command handlers
// ---------------------------------------------------------------------------

/// Merge Kind-1 document sequences into a clone of `body`.
///
/// Drivers such as pymongo send bulk payloads (e.g. `documents` for `insert`,
/// `updates` for `update`, `deletes` for `delete`) as a Kind-1 section rather
/// than embedding them in the Kind-0 body document.  This helper merges them so
/// that command handlers always receive a fully-populated body.
fn merge_doc_sequences_into_body(body: &Document, sections: &[Section]) -> Document {
    let mut merged = body.clone();
    for section in sections {
        if let Section::DocSequence {
            identifier,
            documents,
        } = section
        {
            if !documents.is_empty() {
                let mut arr: bson::Array = Vec::with_capacity(documents.len());
                for d in documents {
                    arr.push(bson::Bson::Document(d.clone()));
                }
                merged.insert(identifier.clone(), bson::Bson::Array(arr));
            }
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
