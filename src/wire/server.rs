//! TCP listener and command handler for the MongoDB wire protocol shim.
//!
//! This module provides [`WireProtocol`], a background server that handles
//! MongoDB wire protocol connections using OP_MSG framing.
//!
//! # Spike scope (hq-23u)
//!
//! Phase 1c requires validating that pymongo can connect before building the
//! full 18-command surface.  This implementation supports the minimal command
//! set needed for a pymongo handshake:
//!
//! - `hello` / `isMaster` — driver handshake
//! - `ping` — connectivity check (`admin.command('ping')`)
//! - `buildInfo` — version metadata
//! - `serverStatus` — runtime diagnostics
//! - `listDatabases` — enumerate the single mqlite database
//!
//! All other commands return `CommandNotFound` (code 59).
//!
//! # Spike finding: two-opcode handshake
//!
//! pymongo 4.x sends the *initial* `isMaster` using OP_QUERY (opcode 2004),
//! the legacy opcode, because at connection time the driver does not yet know
//! the server wire version.  The response must be OP_REPLY (opcode 1).
//!
//! After receiving `helloOk: true` in the OP_REPLY, pymongo switches all
//! subsequent commands — including `hello` topology checks and `ping` — to
//! OP_MSG (opcode 2013).
//!
//! Consequently the server must handle both opcodes:
//! - OP_QUERY → OP_REPLY  (initial handshake only)
//! - OP_MSG   → OP_MSG    (all subsequent commands)

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use bson::{doc, oid::ObjectId, DateTime, Document};

use crate::{database::Database, error::Result};
use super::protocol::{MsgHeader, OpMsg, MAX_MESSAGE_SIZE};

// ---------------------------------------------------------------------------
// Legacy opcodes (not in protocol.rs — used only for handshake interop)
// ---------------------------------------------------------------------------

/// OP_QUERY — legacy opcode used by MongoDB drivers for the *initial*
/// `isMaster` / `hello` handshake before wire version is established.
const OP_QUERY: i32 = 2004;

/// OP_REPLY — legacy response opcode for OP_QUERY messages.
const OP_REPLY: i32 = 1;

// ---------------------------------------------------------------------------
// Cursor idle timeout and per-connection cursor state
// ---------------------------------------------------------------------------

/// Cursors not accessed for longer than this duration are evicted.
const CURSOR_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// A buffered cursor stored in the per-connection cursor map.
#[allow(dead_code)] // cursor field used when data commands (getMore, killCursors) are added
struct StoredCursor {
    /// The buffered cursor data.
    cursor: crate::Cursor<bson::Document>,
    /// When this cursor was last accessed (used for idle eviction).
    last_accessed: std::time::Instant,
}

/// Per-connection cursor state.
///
/// Tracks all open server-side cursors for one TCP connection.  When
/// [`ConnectionCursors`] is dropped (i.e., when the connection closes),
/// every stored cursor is released automatically — satisfying the
/// acceptance criterion "connection close releases all associated cursors".
struct ConnectionCursors {
    /// Cursor ID → stored cursor.
    cursors: HashMap<i64, StoredCursor>,
    /// Monotonically increasing cursor ID counter.  Starts at 1; cursor ID 0
    /// is reserved in the MongoDB wire protocol to mean "no cursor".
    next_cursor_id: i64,
}

#[allow(dead_code)] // methods used when data commands (getMore, killCursors) are added
impl ConnectionCursors {
    fn new() -> Self {
        ConnectionCursors {
            cursors: HashMap::new(),
            next_cursor_id: 1,
        }
    }

    /// Store `cursor` and return its assigned cursor ID.
    fn store(&mut self, cursor: crate::Cursor<bson::Document>) -> i64 {
        let id = self.next_cursor_id;
        // Cursor ID 0 is reserved; skip it on overflow.
        self.next_cursor_id = self.next_cursor_id.wrapping_add(1).max(1);
        self.cursors.insert(
            id,
            StoredCursor {
                cursor,
                last_accessed: std::time::Instant::now(),
            },
        );
        id
    }

    /// Remove and return the cursor identified by `id`, if present.
    fn remove(&mut self, id: i64) -> Option<crate::Cursor<bson::Document>> {
        self.cursors.remove(&id).map(|e| e.cursor)
    }

    /// Return a mutable reference to the cursor for `id`, refreshing its
    /// last-accessed timestamp.  Returns `None` if the cursor is not found.
    fn get_mut(&mut self, id: i64) -> Option<&mut crate::Cursor<bson::Document>> {
        self.cursors.get_mut(&id).map(|e| {
            e.last_accessed = std::time::Instant::now();
            &mut e.cursor
        })
    }

    /// Evict cursors that have been idle for longer than `timeout`.
    ///
    /// Returns the number of cursors evicted.
    fn evict_idle(&mut self, timeout: std::time::Duration) -> usize {
        let before = self.cursors.len();
        self.cursors
            .retain(|_, entry| entry.last_accessed.elapsed() < timeout);
        before - self.cursors.len()
    }

    /// Number of currently open cursors.
    fn len(&self) -> usize {
        self.cursors.len()
    }
}

/// Background task: periodically evict idle cursors for one connection.
///
/// Sweeps every 60 seconds using [`CURSOR_IDLE_TIMEOUT`].  Exits when
/// `shutdown_rx` resolves, which happens automatically when the corresponding
/// sender is dropped (i.e., when the connection handler returns).
async fn cursor_sweep_task(
    cursors: Arc<std::sync::Mutex<ConnectionCursors>>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut c = cursors.lock().unwrap_or_else(|e| e.into_inner());
                let _evicted = c.evict_idle(CURSOR_IDLE_TIMEOUT);
                #[cfg(feature = "tracing")]
                if _evicted > 0 {
                    tracing::debug!(
                        target: "mqlite",
                        evicted = _evicted,
                        "mqlite::wire::cursor_evict"
                    );
                }
            }
            _ = &mut shutdown_rx => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

/// Shared state for the wire protocol server.
///
/// Created once at [`WireProtocol::bind`] time and cloned (cheaply, via
/// `Arc`) into each connection task.  All fields behind `Arc` are shared
/// across every connection so counters are global to the server instance.
#[derive(Clone)]
struct ServerState {
    /// Time when this `WireProtocol` instance was started.
    /// Used to compute uptime in the `serverStatus` response.
    start_time: Arc<std::time::Instant>,

    /// Monotonically increasing counter used to assign unique per-connection IDs.
    /// Starts at 1; each new connection receives the old value before increment.
    next_connection_id: Arc<AtomicI32>,

    /// Path to the database file (`None` for in-memory databases).
    /// Used to locate the WAL file (`<path>-wal`) for `serverStatus`.
    db_path: Option<std::path::PathBuf>,

    /// `topologyVersion.processId` — a random [`ObjectId`] generated once at
    /// server start and included in every `hello` / `isMaster` response.
    topology_process_id: ObjectId,
}

impl Default for ServerState {
    fn default() -> Self {
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: None,
            topology_process_id: ObjectId::new(),
        }
    }
}

impl ServerState {
    /// Create state for a database at the given path.
    fn new(db_path: Option<std::path::PathBuf>) -> Self {
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path,
            topology_process_id: ObjectId::new(),
        }
    }

    /// Reserve and return the next connection ID (pre-increment).
    fn next_conn_id(&self) -> i32 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Return server uptime in whole seconds.
    fn uptime_secs(&self) -> i64 {
        self.start_time.elapsed().as_secs() as i64
    }

    /// Return the size of the WAL file in bytes, or 0 if absent / in-memory.
    fn wal_file_size(&self) -> u64 {
        let wal_path = match &self.db_path {
            Some(p) => {
                let mut s = p.as_os_str().to_owned();
                s.push("-wal");
                std::path::PathBuf::from(s)
            }
            None => return 0,
        };
        std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0)
    }

    /// Total number of connections that have been opened since server start.
    fn total_connections(&self) -> i32 {
        // next_connection_id starts at 1; subtract 1 for the count of allocated IDs.
        self.next_connection_id
            .load(Ordering::Relaxed)
            .saturating_sub(1)
    }

    /// Derive the logical database name from the file path.
    ///
    /// For `/path/to/myapp.mqlite` returns `"myapp"`.
    /// For in-memory databases returns `"local"`.
    fn db_name(&self) -> String {
        self.db_path
            .as_ref()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .map(|s| s.to_owned())
            .unwrap_or_else(|| "local".to_owned())
    }
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
/// use mqlite::{Database, WireProtocol};
///
/// let db = Database::open_in_memory()?;
/// let server = WireProtocol::bind(&db, "127.0.0.1:27017")?;
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
    pub fn bind(db: &Database, addr: &str) -> Result<WireProtocol> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Channel to report bind success/failure back to the caller synchronously.
        let (bind_tx, bind_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();

        // Capture the database path for serverStatus / listDatabases.
        let db_path = db.inner.path.clone();
        let state = ServerState::new(db_path);

        let addr = addr.to_owned();

        // Security: warn when binding to all interfaces — mqlite Phase 1 has
        // no authentication, so 0.0.0.0 exposes the server to the entire
        // network.  Default recommended bind is 127.0.0.1 (localhost only).
        if addr.starts_with("0.0.0.0") {
            eprintln!(
                "mqlite WARNING: wire protocol server bound to {addr} — \
                 accessible from all network interfaces. \
                 Phase 1 has no authentication. \
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
                    _ = accept_loop(listener, state) => {}
                    _ = shutdown_rx => {}
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
async fn accept_loop(listener: tokio::net::TcpListener, state: ServerState) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                let conn_state = state.clone();
                tokio::spawn(handle_connection(stream, conn_state));
            }
            // A hard listener error causes an exit.
            Err(_) => break,
        }
    }
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
        if stream.read_exact(&mut header_buf).await.is_err() {
            break;
        }

        let declared_len =
            i32::from_le_bytes(header_buf[0..4].try_into().expect("slice is 4 bytes")) as usize;
        let opcode =
            i32::from_le_bytes(header_buf[12..16].try_into().expect("slice is 4 bytes"));
        let request_id =
            i32::from_le_bytes(header_buf[4..8].try_into().expect("slice is 4 bytes"));

        // Guard against oversized messages.
        if declared_len < MsgHeader::SIZE || declared_len > MAX_MESSAGE_SIZE {
            break;
        }

        // Read the rest of the message.
        let remainder = declared_len - MsgHeader::SIZE;
        let mut rest = vec![0u8; remainder];
        if stream.read_exact(&mut rest).await.is_err() {
            break;
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
                match dispatch_op_msg(&msg, next_request_id, request_id, &state, connection_id) {
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
// OP_QUERY parsing and OP_REPLY generation
// ---------------------------------------------------------------------------

/// Parse an OP_QUERY message and return the command document.
///
/// OP_QUERY layout (after the 16-byte header):
/// ```text
/// flags             : int32
/// fullCollectionName: cstring  (null-terminated)
/// numberToSkip      : int32
/// numberToReturn    : int32
/// query             : BSON document  (the command)
/// [returnFieldsSelector: BSON document]  (optional; ignored)
/// ```
fn parse_op_query_body(buf: &[u8]) -> Result<Document> {
    if buf.len() < 4 {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY body too short for flags".into(),
        });
    }
    // Skip flags (4 bytes), then find the null terminator of fullCollectionName.
    let after_flags = &buf[4..];
    let null_pos =
        after_flags
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| crate::error::Error::InvalidWireMessage {
                detail: "OP_QUERY fullCollectionName not null-terminated".into(),
            })?;
    // Skip the null terminator, then skip numberToSkip (4) and numberToReturn (4).
    let doc_offset = 4 + null_pos + 1 + 4 + 4;
    if doc_offset + 4 > buf.len() {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY body too short for query document".into(),
        });
    }
    let doc_size =
        i32::from_le_bytes(buf[doc_offset..doc_offset + 4].try_into().expect("4 bytes")) as usize;
    if doc_offset + doc_size > buf.len() {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: format!(
                "OP_QUERY document size {} exceeds remaining buffer",
                doc_size
            ),
        });
    }
    let raw = bson::RawDocumentBuf::from_bytes(buf[doc_offset..doc_offset + doc_size].to_vec())
        .map_err(|e| crate::error::Error::InvalidWireMessage {
            detail: format!("OP_QUERY BSON parse error: {}", e),
        })?;
    bson::from_slice::<Document>(raw.as_bytes()).map_err(|e| {
        crate::error::Error::InvalidWireMessage {
            detail: format!("OP_QUERY BSON deserialise error: {}", e),
        }
    })
}

/// Build an OP_REPLY response for an OP_QUERY request.
///
/// OP_REPLY layout:
/// ```text
/// MsgHeader      (16 bytes)
/// responseFlags  : int32   (0 = no flags)
/// cursorID       : int64   (0 = no cursor)
/// startingFrom   : int32   (0)
/// numberReturned : int32   (1)
/// document       : BSON
/// ```
fn build_op_reply(request_id: i32, response_to: i32, body: &Document) -> Result<Vec<u8>> {
    let bson_bytes = bson::to_vec(body)?;
    // header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
    let total = 16 + 4 + 8 + 4 + 4 + bson_bytes.len();
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to,
        op_code: OP_REPLY,
    };
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&header.to_bytes());
    out.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
    out.extend_from_slice(&0i64.to_le_bytes()); // cursorID
    out.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    out.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    out.extend_from_slice(&bson_bytes);
    Ok(out)
}

// ---------------------------------------------------------------------------
// $db validation helpers
// ---------------------------------------------------------------------------

/// Extract the database name from an OP_QUERY body buffer.
///
/// OP_QUERY body layout (after the 16-byte `MsgHeader`):
/// ```text
/// flags             : int32
/// fullCollectionName: cstring  (e.g. "admin.$cmd")
/// numberToSkip      : int32
/// numberToReturn    : int32
/// query             : BSON document
/// ```
///
/// Returns the part of `fullCollectionName` before the first `'.'` (the
/// database name), or `None` if the buffer is too short or not valid UTF-8.
fn parse_op_query_db_name(buf: &[u8]) -> Option<String> {
    if buf.len() < 5 {
        return None;
    }
    // Skip 4-byte flags field.
    let after_flags = &buf[4..];
    // Locate the null terminator of fullCollectionName.
    let null_pos = after_flags.iter().position(|&b| b == 0)?;
    let coll_name = std::str::from_utf8(&after_flags[..null_pos]).ok()?;
    // Database name is the component before the first '.'.
    Some(coll_name.split('.').next().unwrap_or("").to_owned())
}

/// Validate the `$db` field in an OP_MSG command body.
///
/// Returns `Some(error_doc)` when `$db` is present and does not match
/// `server_db_name` or `"admin"` (which is always permitted for server-level
/// commands such as `hello`, `ping`, `buildInfo`, etc.).
///
/// Returns `None` when the field is absent, not a string, empty, or valid.
fn check_db_field(body: &Document, server_db_name: &str) -> Option<Document> {
    let db = match body.get_str("$db") {
        Ok(s) => s,
        Err(_) => return None, // absent or wrong BSON type — allow
    };
    if db.is_empty() || db == "admin" || db == server_db_name {
        return None; // valid
    }
    Some(doc! {
        "ok": 0.0_f64,
        "errmsg": format!("not authorized on {} to execute command", db),
        "code": 13i32,
        "codeName": "Unauthorized",
    })
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

    // Validate database from fullCollectionName (e.g. "admin.$cmd").
    if let Some(ref query_db) = parse_op_query_db_name(body_buf) {
        if !query_db.is_empty() && query_db != "admin" && *query_db != state.db_name() {
            let err = doc! {
                "ok": 0.0_f64,
                "errmsg": format!("not authorized on {} to execute command", query_db),
                "code": 13i32,
                "codeName": "Unauthorized",
            };
            return build_op_reply(request_id, response_to, &err);
        }
    }

    let doc = parse_op_query_body(body_buf)?;
    let command_name = doc
        .keys()
        .next()
        .ok_or_else(|| crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY command document is empty".into(),
        })?;
    let response_body = route_command(command_name, &doc, state, connection_id);
    build_op_reply(request_id, response_to, &response_body)
}

/// Dispatch an OP_MSG message, returning a serialised OP_MSG response.
fn dispatch_op_msg(
    msg: &OpMsg,
    request_id: i32,
    response_to: i32,
    state: &ServerState,
    connection_id: i32,
) -> Result<Vec<u8>> {
    let body = msg
        .body()
        .ok_or_else(|| crate::error::Error::InvalidWireMessage {
            detail: "command message has no Kind-0 body section".into(),
        })?;
    let command_name = body
        .keys()
        .next()
        .ok_or_else(|| crate::error::Error::InvalidWireMessage {
            detail: "command body document is empty".into(),
        })?;
    // Validate $db before routing.  Returns Unauthorized (code 13) when
    // $db is present and does not match "admin" or the server's db name.
    if let Some(err) = check_db_field(body, &state.db_name()) {
        return OpMsg::build_response(request_id, response_to, &err);
    }
    let response_body = route_command(command_name, body, state, connection_id);
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
) -> Document {
    // Silently log (and ignore) fields that mqlite does not support.
    // Per integration.md: lsid, readConcern, writeConcern, $clusterTime, txnNumber
    // are silently ignored in Phase 1 — log at DEBUG, never return error.
    #[cfg(feature = "tracing")]
    {
        for key in ["lsid", "readConcern", "writeConcern", "$clusterTime", "txnNumber"] {
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
        "hello" | "ismaster" => handle_hello(state, connection_id),
        "ping" => handle_ping(),
        "buildinfo" => handle_build_info(),
        "serverstatus" => handle_server_status(state),
        "listdatabases" => handle_list_databases(state),
        other => handle_unknown(other),
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

    // Suppress unused-variable warning when tracing feature is disabled.
    let _ = body;

    result
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/// `hello` / `isMaster` — driver handshake response.
///
/// Reports server capabilities to the driver.  We advertise:
/// - Standalone mode (`isWritablePrimary: true`, no replica set fields)
/// - `maxWireVersion: 21` (MongoDB 8.0) to prevent driver downgrades
/// - `helloOk: true` — signals to pymongo and mongosh that the server supports
///   the `hello` command, so subsequent topology checks use `hello` via OP_MSG
/// - `topologyVersion` — required by MongoDB 5.0+ drivers; `processId` is
///   generated once at server start, `counter` is always 0 (not a replica set)
/// - `connectionId` — unique per-connection integer identifier
/// - No sessions, no auth, no transactions — strips capabilities mqlite lacks
/// - `mqlite.version` so tooling can detect it is talking to mqlite
///
/// See integration.md §Handshake Response Design for the full field rationale.
fn handle_hello(state: &ServerState, connection_id: i32) -> Document {
    doc! {
        // Standalone — no replica set discovery.
        "isWritablePrimary": true,

        // Signals that the server supports `hello` — pymongo 4.x will use
        // `hello` via OP_MSG for all subsequent topology checks instead of
        // retrying with legacy `isMaster` via OP_QUERY.
        "helloOk": true,

        // Topology version — required by MongoDB 5.0+ drivers.
        // processId is a random ObjectId generated once at server start.
        // counter is always 0 (mqlite is not a replica set and never transitions state).
        "topologyVersion": {
            "processId": state.topology_process_id,
            "counter": bson::Bson::Int64(0_i64),
        },

        // Capacity limits (match MongoDB 8.0 defaults).
        "maxBsonObjectSize": 16_777_216i32,
        "maxMessageSizeBytes": 48_000_000i32,
        "maxWriteBatchSize": 100_000i32,

        // Current server time (used by drivers for clock skew detection).
        "localTime": DateTime::now(),

        // Unique identifier for this connection.
        "connectionId": connection_id,

        // Wire protocol version range.
        // minWireVersion: 0  — accept all drivers.
        // maxWireVersion: 21 — MongoDB 8.0; prevents drivers from trying
        //                      to negotiate down to legacy opcodes.
        "minWireVersion": 0i32,
        "maxWireVersion": 21i32,

        // Not a read-only replica.
        "readOnly": false,

        // mqlite identity — lets client code detect it is talking to mqlite.
        "mqlite": {
            "version": env!("CARGO_PKG_VERSION"),
        },

        "ok": 1.0_f64,
    }
}

/// `ping` — basic connectivity check.
///
/// Returns `{ ok: 1 }`.
fn handle_ping() -> Document {
    doc! { "ok": 1.0_f64 }
}

/// `buildInfo` — server build metadata.
///
/// Returns mqlite version information in MongoDB buildInfo format.
/// See integration.md §Server Version Reporting for field rationale.
fn handle_build_info() -> Document {
    doc! {
        "version": env!("CARGO_PKG_VERSION"),
        "gitVersion": env!("CARGO_PKG_VERSION"),
        // Empty modules array — mqlite has no enterprise/community modules.
        "modules": [],
        // Memory allocator identity.
        "allocator": "rust",
        // Identify this as mqlite, not MongoDB.
        "mqlite": true,
        "ok": 1.0_f64,
    }
}

/// `serverStatus` — runtime diagnostic statistics.
///
/// Returns uptime, WAL file size, connection count, and placeholder buffer pool
/// stats sourced from internal server state (not the public `stats()` API,
/// which is deferred to Phase 2 per api.md).
///
/// Data sources:
/// - **uptime**: elapsed seconds since `WireProtocol::bind()`.
/// - **WAL size**: `std::fs::metadata("<db>-wal")?.len()` — best-effort, 0 if absent.
/// - **connections.current**: approximate (counts connections opened, not live ones).
/// - **buffer pool**: placeholder zeros (pool instrumentation is Phase 2).
fn handle_server_status(state: &ServerState) -> Document {
    let uptime_secs = state.uptime_secs();
    let wal_size = state.wal_file_size() as i64;
    let total_conns = state.total_connections();

    doc! {
        "host": "mqlite",
        "version": env!("CARGO_PKG_VERSION"),
        "process": "mqlite",
        "pid": bson::Bson::Int64(std::process::id() as i64),
        "uptime": bson::Bson::Int64(uptime_secs),
        "uptimeMillis": bson::Bson::Int64(uptime_secs * 1_000),
        "uptimeEstimate": bson::Bson::Int64(uptime_secs),
        "localTime": DateTime::now(),
        "connections": {
            "current": total_conns,
            "available": 64i32,
            "totalCreated": total_conns,
        },
        "storageEngine": {
            "name": "mqlite",
            "persistent": true,
            "supportsCommittedReads": false,
            "readOnly": false,
        },
        // WAL-based storage stats.
        "mqlite": {
            "walFileSizeBytes": wal_size,
        },
        // Placeholder buffer pool stats (Phase 2: pool instrumentation).
        "bufferPool": {
            "hits": 0i64,
            "misses": 0i64,
            "pages": 0i64,
        },
        "ok": 1.0_f64,
    }
}

/// `listDatabases` — enumerate available databases.
///
/// mqlite is single-database per file.  The database name is derived from the
/// file stem of the `.mqlite` path (e.g., `myapp.mqlite` → `"myapp"`).
/// In-memory databases are named `"local"`.
fn handle_list_databases(state: &ServerState) -> Document {
    let db_name = state.db_name();

    // Approximate size: WAL file size is a proxy for recent write activity.
    // Actual on-disk size requires stat() on the main file, which is best-effort.
    let size_on_disk = state.wal_file_size() as i64;

    doc! {
        "databases": [
            {
                "name": &db_name,
                "sizeOnDisk": size_on_disk,
                "empty": false,
            }
        ],
        "totalSize": size_on_disk,
        "totalSizeMb": bson::Bson::Int64(size_on_disk / (1024 * 1024)),
        "ok": 1.0_f64,
    }
}

/// Unknown command — returns `CommandNotFound` (error code 59).
fn handle_unknown(name: &str) -> Document {
    #[cfg(feature = "tracing")]
    tracing::warn!(
        target: "mqlite",
        operator = name,
        "mqlite::unsupported_op"
    );
    doc! {
        "ok": 0.0_f64,
        "errmsg": format!("no such command: '{}'", name),
        "code": 59i32,
        "codeName": "CommandNotFound",
    }
}

// ---------------------------------------------------------------------------
// Async framing helpers (public; used by integration tests and benchmarks)
// ---------------------------------------------------------------------------

/// Read exactly one complete OP_MSG message from `stream`.
///
/// # Errors
///
/// - `Io` — network error
/// - `InvalidWireMessage` — header too short, opcode not supported, message
///   exceeds `MAX_MESSAGE_SIZE`, or checksum mismatch
pub async fn read_message(stream: &mut TcpStream) -> Result<OpMsg> {
    // Step 1: read the 16-byte header.
    let mut header_buf = [0u8; MsgHeader::SIZE];
    stream.read_exact(&mut header_buf).await?;

    // Peek at the declared message length without fully parsing the header yet.
    let declared_len = i32::from_le_bytes(header_buf[0..4].try_into().unwrap()) as usize;

    if declared_len < MsgHeader::SIZE {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: format!(
                "messageLength {} is smaller than header size {}",
                declared_len,
                MsgHeader::SIZE
            ),
        });
    }
    if declared_len > MAX_MESSAGE_SIZE {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: format!(
                "message size {} exceeds maximum {} bytes (48 MiB)",
                declared_len, MAX_MESSAGE_SIZE
            ),
        });
    }

    // Step 2: allocate a buffer for the full message and copy the header in.
    let mut msg_buf = vec![0u8; declared_len];
    msg_buf[..MsgHeader::SIZE].copy_from_slice(&header_buf);

    // Step 3: read the remainder of the message.
    stream.read_exact(&mut msg_buf[MsgHeader::SIZE..]).await?;

    // Step 4: parse and validate.
    OpMsg::parse(&msg_buf)
}

/// Write a pre-serialised OP_MSG response to `stream`.
pub async fn write_message(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use tokio::net::{TcpListener, TcpStream as TokioStream};

    /// Helper: spin up a loopback TCP pair and return (client, server) streams.
    async fn loopback_pair() -> (TokioStream, TokioStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TokioStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    // -----------------------------------------------------------------------
    // Framing helpers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_write_round_trip() {
        let (mut client, mut server) = loopback_pair().await;

        let body = doc! { "ok": 1, "ismaster": true };
        let bytes = OpMsg::build_response(1, 99, &body).unwrap();
        write_message(&mut server, &bytes).await.unwrap();

        let msg = read_message(&mut client).await.unwrap();
        assert_eq!(msg.header.request_id, 1);
        assert_eq!(msg.header.response_to, 99);
        let parsed_body = msg.body().unwrap();
        assert_eq!(parsed_body.get_i32("ok").unwrap(), 1);
    }

    #[tokio::test]
    async fn oversized_message_rejected_on_read() {
        let (mut client, mut server) = loopback_pair().await;

        let claimed = (49usize * 1024 * 1024) as i32;
        let header = MsgHeader {
            message_length: claimed,
            request_id: 1,
            response_to: 0,
            op_code: super::super::protocol::OP_MSG,
        };
        server.write_all(&header.to_bytes()).await.unwrap();

        let err = read_message(&mut client).await.unwrap_err();
        match err {
            crate::error::Error::InvalidWireMessage { detail } => {
                assert!(
                    detail.contains("exceeds maximum") || detail.contains("48 MiB"),
                    "got: {}",
                    detail
                );
            }
            _ => panic!("wrong error type: {:?}", err),
        }
    }

    // -----------------------------------------------------------------------
    // Command dispatch (unit tests — no network)
    // -----------------------------------------------------------------------

    /// Build a minimal OP_MSG request carrying `body`.
    fn make_op_msg_request(request_id: i32, body: &Document) -> Vec<u8> {
        let bson_bytes = bson::to_vec(body).unwrap();
        let total = MsgHeader::SIZE + 4 + 1 + bson_bytes.len();
        let header = MsgHeader {
            message_length: total as i32,
            request_id,
            response_to: 0,
            op_code: super::super::protocol::OP_MSG,
        };
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits = 0
        buf.push(0); // Kind-0
        buf.extend_from_slice(&bson_bytes);
        buf
    }

    /// Build a minimal OP_QUERY request.
    fn make_op_query_request(request_id: i32, collection: &str, body: &Document) -> Vec<u8> {
        let bson_bytes = bson::to_vec(body).unwrap();
        let coll_bytes = {
            let mut v = collection.as_bytes().to_vec();
            v.push(0); // null terminator
            v
        };
        // header(16) + flags(4) + coll + skip(4) + nret(4) + doc
        let total = 16 + 4 + coll_bytes.len() + 4 + 4 + bson_bytes.len();
        let header = MsgHeader {
            message_length: total as i32,
            request_id,
            response_to: 0,
            op_code: OP_QUERY,
        };
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(&0i32.to_le_bytes()); // flags
        buf.extend_from_slice(&coll_bytes);
        buf.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
        buf.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
        buf.extend_from_slice(&bson_bytes);
        buf
    }

    #[test]
    fn dispatch_op_msg_ping() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(1, &doc! { "ping": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 10, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn dispatch_op_msg_hello() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(2, &doc! { "hello": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 11, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
        assert!(body.get_bool("helloOk").unwrap());
        assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
        assert_eq!(body.get_i32("minWireVersion").unwrap(), 0);
        // connectionId must be present and match the value passed.
        assert_eq!(body.get_i32("connectionId").unwrap(), 1);
        // topologyVersion must be present with processId and counter=0.
        let tv = body.get_document("topologyVersion").unwrap();
        assert!(tv.contains_key("processId"));
        assert_eq!(tv.get_i64("counter").unwrap(), 0);
    }

    #[test]
    fn dispatch_op_query_ismaster() {
        let state = ServerState::default();
        let req_buf = make_op_query_request(
            3,
            "admin.$cmd",
            &doc! { "ismaster": 1, "helloOk": true },
        );
        let resp_bytes = dispatch_op_query(&req_buf, 12, 3, &state, 2).unwrap();

        // Response must be OP_REPLY (opcode 1).
        let header = MsgHeader::parse(&resp_bytes).unwrap();
        assert_eq!(header.op_code, OP_REPLY);
        assert_eq!(header.response_to, 3);

        // Parse the OP_REPLY body.
        // Layout: header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
        let doc_start = 16 + 4 + 8 + 4 + 4;
        let doc_size = i32::from_le_bytes(
            resp_bytes[doc_start..doc_start + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let raw = bson::RawDocumentBuf::from_bytes(
            resp_bytes[doc_start..doc_start + doc_size].to_vec(),
        )
        .unwrap();
        let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
        assert!(body.get_bool("helloOk").unwrap());
        // topologyVersion must be present.
        assert!(body.contains_key("topologyVersion"));
        // connectionId must be present.
        assert!(body.contains_key("connectionId"));
    }

    #[test]
    fn dispatch_op_msg_ismaster() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(3, &doc! { "ismaster": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 12, msg.header.request_id, &state, 3).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert!(body.get_bool("isWritablePrimary").unwrap());
        assert_eq!(body.get_i32("connectionId").unwrap(), 3);
    }

    #[test]
    fn dispatch_op_msg_build_info() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(4, &doc! { "buildInfo": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 13, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_str("version").is_ok());
        // modules must be an empty array.
        let modules = body.get_array("modules").unwrap();
        assert!(modules.is_empty());
        // allocator field.
        assert_eq!(body.get_str("allocator").unwrap(), "rust");
        // mqlite: true identity marker.
        assert!(body.get_bool("mqlite").unwrap());
    }

    #[test]
    fn dispatch_op_msg_server_status() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(5, &doc! { "serverStatus": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 14, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        // uptime must be non-negative.
        assert!(body.get_i64("uptime").unwrap() >= 0);
        // connections sub-document must be present.
        assert!(body.contains_key("connections"));
        // storageEngine sub-document must be present.
        let se = body.get_document("storageEngine").unwrap();
        assert_eq!(se.get_str("name").unwrap(), "mqlite");
    }

    #[test]
    fn dispatch_op_msg_list_databases() {
        let state = ServerState::default();
        let req_buf =
            make_op_msg_request(6, &doc! { "listDatabases": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 15, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        // Must contain exactly one database entry.
        let dbs = body.get_array("databases").unwrap();
        assert_eq!(dbs.len(), 1);
        // The entry must have a "name" field.
        let db_doc = dbs[0].as_document().unwrap();
        assert!(db_doc.contains_key("name"));
    }

    #[test]
    fn dispatch_op_msg_unknown_command() {
        let state = ServerState::default();
        // Use $db: "admin" (always allowed) to test CommandNotFound, not Unauthorized.
        let req_buf = make_op_msg_request(7, &doc! { "aggregate": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 16, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 0.0);
        assert_eq!(body.get_i32("code").unwrap(), 59);
        assert_eq!(body.get_str("codeName").unwrap(), "CommandNotFound");
    }

    // -----------------------------------------------------------------------
    // $db field validation
    // -----------------------------------------------------------------------

    #[test]
    fn check_db_field_missing_is_allowed() {
        // No $db field → allowed (backward compat with older drivers)
        let body = doc! { "ping": 1 };
        assert!(check_db_field(&body, "myapp").is_none());
    }

    #[test]
    fn check_db_field_admin_is_allowed() {
        // $db: "admin" is always permitted for server-level commands.
        let body = doc! { "ping": 1, "$db": "admin" };
        assert!(check_db_field(&body, "myapp").is_none());
    }

    #[test]
    fn check_db_field_matching_is_allowed() {
        // $db matching the actual db name is allowed.
        let body = doc! { "ping": 1, "$db": "myapp" };
        assert!(check_db_field(&body, "myapp").is_none());
    }

    #[test]
    fn check_db_field_mismatch_returns_unauthorized() {
        let body = doc! { "ping": 1, "$db": "wrongdb" };
        let err = check_db_field(&body, "myapp").expect("expected Unauthorized doc");
        assert_eq!(err.get_f64("ok").unwrap(), 0.0);
        assert_eq!(err.get_i32("code").unwrap(), 13);
        assert_eq!(err.get_str("codeName").unwrap(), "Unauthorized");
        assert!(err.get_str("errmsg").is_ok());
    }

    #[test]
    fn dispatch_op_msg_db_mismatch_returns_unauthorized() {
        let state = ServerState::default(); // db_name() == "local"
        // "wrongdb" != "admin" and "wrongdb" != "local" → Unauthorized
        let req_buf = make_op_msg_request(20, &doc! { "ping": 1, "$db": "wrongdb" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 40, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 0.0);
        assert_eq!(body.get_i32("code").unwrap(), 13);
        assert_eq!(body.get_str("codeName").unwrap(), "Unauthorized");
    }

    #[test]
    fn dispatch_op_msg_db_admin_always_allowed() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(21, &doc! { "ping": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 41, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn dispatch_op_msg_db_matching_allowed() {
        let state = ServerState::new(Some(std::path::PathBuf::from("/tmp/myapp.mqlite")));
        let req_buf = make_op_msg_request(22, &doc! { "ping": 1, "$db": "myapp" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 42, msg.header.request_id, &state, 1).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    // -----------------------------------------------------------------------
    // parse_op_query_db_name
    // -----------------------------------------------------------------------

    #[test]
    fn parse_op_query_db_name_admin_cmd() {
        // Simulate OP_QUERY body with fullCollectionName = "admin.$cmd"
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&0i32.to_le_bytes()); // flags
        buf.extend_from_slice(b"admin.$cmd\x00"); // fullCollectionName + NUL
        buf.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
        buf.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
        assert_eq!(parse_op_query_db_name(&buf).as_deref(), Some("admin"));
    }

    #[test]
    fn parse_op_query_db_name_custom_collection() {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(b"myapp.users\x00");
        buf.extend_from_slice(&0i32.to_le_bytes());
        buf.extend_from_slice(&(-1i32).to_le_bytes());
        assert_eq!(parse_op_query_db_name(&buf).as_deref(), Some("myapp"));
    }

    #[test]
    fn dispatch_op_query_db_mismatch_returns_unauthorized() {
        let state = ServerState::default(); // db_name() == "local"
        // "wrongdb.$cmd" → db = "wrongdb" ≠ "admin" and ≠ "local"
        let req_buf = make_op_query_request(30, "wrongdb.$cmd", &doc! { "ismaster": 1 });
        let resp_bytes = dispatch_op_query(&req_buf, 50, 30, &state, 1).unwrap();
        // OP_REPLY: header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
        let doc_start = 16 + 4 + 8 + 4 + 4;
        let doc_size = i32::from_le_bytes(
            resp_bytes[doc_start..doc_start + 4].try_into().unwrap(),
        ) as usize;
        let raw =
            bson::RawDocumentBuf::from_bytes(resp_bytes[doc_start..doc_start + doc_size].to_vec())
                .unwrap();
        let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 0.0);
        assert_eq!(body.get_i32("code").unwrap(), 13);
        assert_eq!(body.get_str("codeName").unwrap(), "Unauthorized");
    }

    // -----------------------------------------------------------------------
    // ConnectionCursors unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn connection_cursors_new_is_empty() {
        let state = ConnectionCursors::new();
        assert_eq!(state.len(), 0);
    }

    #[test]
    fn connection_cursors_store_and_remove() {
        let mut state = ConnectionCursors::new();
        let cursor = crate::Cursor::<bson::Document>::empty();
        let id = state.store(cursor);
        assert_eq!(id, 1, "first cursor should get ID 1");
        assert_eq!(state.len(), 1);

        // Removing an existing cursor returns Some.
        assert!(state.remove(id).is_some());
        assert_eq!(state.len(), 0);

        // Removing again returns None.
        assert!(state.remove(id).is_none());
    }

    #[test]
    fn connection_cursors_sequential_ids() {
        let mut state = ConnectionCursors::new();
        let id1 = state.store(crate::Cursor::<bson::Document>::empty());
        let id2 = state.store(crate::Cursor::<bson::Document>::empty());
        let id3 = state.store(crate::Cursor::<bson::Document>::empty());
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn connection_cursors_evict_zero_timeout_removes_all() {
        let mut state = ConnectionCursors::new();
        state.store(crate::Cursor::<bson::Document>::empty());
        state.store(crate::Cursor::<bson::Document>::empty());
        assert_eq!(state.len(), 2);

        // Zero timeout: every cursor is "idle".
        let evicted = state.evict_idle(std::time::Duration::from_secs(0));
        assert_eq!(evicted, 2);
        assert_eq!(state.len(), 0);
    }

    #[test]
    fn connection_cursors_evict_long_timeout_keeps_all() {
        let mut state = ConnectionCursors::new();
        state.store(crate::Cursor::<bson::Document>::empty());
        state.store(crate::Cursor::<bson::Document>::empty());

        // Very long timeout: nothing evicted.
        let evicted = state.evict_idle(std::time::Duration::from_secs(3600));
        assert_eq!(evicted, 0);
        assert_eq!(state.len(), 2);
    }

    #[test]
    fn connection_cursors_get_mut_existing_and_missing() {
        let mut state = ConnectionCursors::new();
        let id = state.store(crate::Cursor::<bson::Document>::empty());
        assert!(state.get_mut(id).is_some());
        assert!(state.get_mut(999).is_none());
    }

    // -----------------------------------------------------------------------
    // hello response — spec compliance
    // -----------------------------------------------------------------------

    #[test]
    fn hello_topology_version_fields() {
        // topologyVersion must have a processId (ObjectId) and counter (Int64 = 0).
        let state = ServerState::default();
        let body = handle_hello(&state, 42);

        let tv = body.get_document("topologyVersion").unwrap();
        // processId must be an ObjectId.
        assert!(
            matches!(tv.get("processId"), Some(bson::Bson::ObjectId(_))),
            "processId should be an ObjectId, got: {:?}",
            tv.get("processId")
        );
        assert_eq!(tv.get_i64("counter").unwrap(), 0);
        // connectionId must match the argument.
        assert_eq!(body.get_i32("connectionId").unwrap(), 42);
    }

    #[test]
    fn hello_topology_process_id_stable() {
        // Two calls on the same ServerState must return the same processId.
        let state = ServerState::default();
        let body1 = handle_hello(&state, 1);
        let body2 = handle_hello(&state, 2);
        let pid1 = body1
            .get_document("topologyVersion")
            .unwrap()
            .get("processId")
            .cloned();
        let pid2 = body2
            .get_document("topologyVersion")
            .unwrap()
            .get("processId")
            .cloned();
        assert_eq!(pid1, pid2, "topology processId should be stable across calls");
    }

    #[test]
    fn hello_connection_ids_unique_per_connection() {
        // Two connections on the same ServerState must get different connectionIds.
        let state = ServerState::default();
        let id1 = state.next_conn_id();
        let id2 = state.next_conn_id();
        assert_ne!(id1, id2);
    }

    // -----------------------------------------------------------------------
    // buildInfo — spec compliance
    // -----------------------------------------------------------------------

    #[test]
    fn build_info_required_fields() {
        let body = handle_build_info();
        assert!(body.get_str("version").is_ok());
        assert!(body.get_str("gitVersion").is_ok());
        assert_eq!(body.get_str("allocator").unwrap(), "rust");
        assert!(body.get_bool("mqlite").unwrap());
        assert!(body.get_array("modules").unwrap().is_empty());
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    // -----------------------------------------------------------------------
    // serverStatus — spec compliance
    // -----------------------------------------------------------------------

    #[test]
    fn server_status_required_fields() {
        let state = ServerState::default();
        let body = handle_server_status(&state);
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_i64("uptime").unwrap() >= 0);
        assert!(body.get_i64("uptimeMillis").unwrap() >= 0);
        assert!(body.contains_key("connections"));
        assert!(body.contains_key("storageEngine"));
        assert!(body.contains_key("localTime"));
    }

    // -----------------------------------------------------------------------
    // listDatabases — spec compliance
    // -----------------------------------------------------------------------

    #[test]
    fn list_databases_single_entry() {
        let state = ServerState::default();
        let body = handle_list_databases(&state);
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        let dbs = body.get_array("databases").unwrap();
        assert_eq!(dbs.len(), 1, "mqlite must list exactly one database");
        let db_doc = dbs[0].as_document().unwrap();
        assert!(db_doc.contains_key("name"), "database entry must have a name");
        assert!(db_doc.contains_key("sizeOnDisk"), "database entry must have sizeOnDisk");
        assert!(db_doc.contains_key("empty"), "database entry must have empty");
    }

    #[test]
    fn list_databases_name_from_path() {
        // When a db_path is provided the name should be the file stem.
        let state = ServerState::new(Some(std::path::PathBuf::from("/tmp/myapp.mqlite")));
        let body = handle_list_databases(&state);
        let dbs = body.get_array("databases").unwrap();
        let db_doc = dbs[0].as_document().unwrap();
        assert_eq!(db_doc.get_str("name").unwrap(), "myapp");
    }

    #[test]
    fn list_databases_in_memory_name() {
        // In-memory (no path) returns "local".
        let state = ServerState::new(None);
        let body = handle_list_databases(&state);
        let dbs = body.get_array("databases").unwrap();
        let db_doc = dbs[0].as_document().unwrap();
        assert_eq!(db_doc.get_str("name").unwrap(), "local");
    }

    // -----------------------------------------------------------------------
    // Integration: WireProtocol::bind + full TCP round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn wire_protocol_bind_and_ping() {
        // Pick a random port to avoid conflicts.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let db = Database::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&db, &addr.to_string()).unwrap();

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Send an OP_MSG ping.
        let body = bson::to_vec(&doc! { "ping": 1, "$db": "admin" }).unwrap();
        let total = (MsgHeader::SIZE + 4 + 1 + body.len()) as i32;
        let header = MsgHeader {
            message_length: total,
            request_id: 42,
            response_to: 0,
            op_code: super::super::protocol::OP_MSG,
        };
        use std::io::{Read, Write};
        client.write_all(&header.to_bytes()).unwrap();
        client.write_all(&0u32.to_le_bytes()).unwrap(); // flagBits
        client.write_all(&[0u8]).unwrap(); // Kind-0
        client.write_all(&body).unwrap();

        // Read response.
        let mut hbuf = [0u8; MsgHeader::SIZE];
        client.read_exact(&mut hbuf).unwrap();
        let resp_header = MsgHeader::parse(&hbuf).unwrap();
        assert_eq!(resp_header.response_to, 42);
        assert_eq!(resp_header.op_code, super::super::protocol::OP_MSG);

        let remaining = resp_header.message_length as usize - MsgHeader::SIZE;
        let mut rest = vec![0u8; remaining];
        client.read_exact(&mut rest).unwrap();

        let mut full = hbuf.to_vec();
        full.extend_from_slice(&rest);
        let resp_msg = OpMsg::parse(&full).unwrap();
        let resp_body = resp_msg.body().unwrap();
        assert_eq!(resp_body.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn wire_protocol_op_query_ismaster_round_trip() {
        // Pick a random port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let db = Database::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&db, &addr.to_string()).unwrap();

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Send an OP_QUERY isMaster (like pymongo does on initial connect).
        let body_doc = doc! { "ismaster": 1, "helloOk": true };
        let body_bson = bson::to_vec(&body_doc).unwrap();
        let coll = b"admin.$cmd\x00";
        let total = (16 + 4 + coll.len() + 4 + 4 + body_bson.len()) as i32;
        let header = MsgHeader {
            message_length: total,
            request_id: 7,
            response_to: 0,
            op_code: OP_QUERY,
        };
        use std::io::{Read, Write};
        client.write_all(&header.to_bytes()).unwrap();
        client.write_all(&0i32.to_le_bytes()).unwrap(); // flags
        client.write_all(coll).unwrap();
        client.write_all(&0i32.to_le_bytes()).unwrap(); // numberToSkip
        client.write_all(&(-1i32).to_le_bytes()).unwrap(); // numberToReturn
        client.write_all(&body_bson).unwrap();

        // Read OP_REPLY response.
        let mut hbuf = [0u8; MsgHeader::SIZE];
        client.read_exact(&mut hbuf).unwrap();
        let resp_header = MsgHeader::parse(&hbuf).unwrap();
        assert_eq!(resp_header.op_code, OP_REPLY);
        assert_eq!(resp_header.response_to, 7);

        // Skip responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) = 20 bytes
        let remaining = resp_header.message_length as usize - 16;
        let mut rest = vec![0u8; remaining];
        client.read_exact(&mut rest).unwrap();

        // BSON doc starts at offset 20 within rest.
        let doc_start = 20;
        let doc_size = i32::from_le_bytes(rest[doc_start..doc_start + 4].try_into().unwrap()) as usize;
        let raw = bson::RawDocumentBuf::from_bytes(rest[doc_start..doc_start + doc_size].to_vec()).unwrap();
        let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();

        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
        assert!(body.get_bool("helloOk").unwrap());
        assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
        // topologyVersion and connectionId must be present in OP_QUERY response too.
        assert!(body.contains_key("topologyVersion"));
        assert!(body.contains_key("connectionId"));
    }

    // -----------------------------------------------------------------------
    // serverStatus — integration via WireProtocol bind
    // -----------------------------------------------------------------------

    #[test]
    fn wire_protocol_server_status_round_trip() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let db = Database::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&db, &addr.to_string()).unwrap();

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let body_bson = bson::to_vec(&doc! { "serverStatus": 1, "$db": "admin" }).unwrap();
        let total = (MsgHeader::SIZE + 4 + 1 + body_bson.len()) as i32;
        let header = MsgHeader {
            message_length: total,
            request_id: 10,
            response_to: 0,
            op_code: super::super::protocol::OP_MSG,
        };
        use std::io::{Read, Write};
        client.write_all(&header.to_bytes()).unwrap();
        client.write_all(&0u32.to_le_bytes()).unwrap(); // flagBits
        client.write_all(&[0u8]).unwrap(); // Kind-0
        client.write_all(&body_bson).unwrap();

        let mut hbuf = [0u8; MsgHeader::SIZE];
        client.read_exact(&mut hbuf).unwrap();
        let resp_header = MsgHeader::parse(&hbuf).unwrap();
        let remaining = resp_header.message_length as usize - MsgHeader::SIZE;
        let mut rest = vec![0u8; remaining];
        client.read_exact(&mut rest).unwrap();

        let mut full = hbuf.to_vec();
        full.extend_from_slice(&rest);
        let resp_msg = OpMsg::parse(&full).unwrap();
        let resp_body = resp_msg.body().unwrap();
        assert_eq!(resp_body.get_f64("ok").unwrap(), 1.0);
        assert!(resp_body.get_i64("uptime").unwrap() >= 0);
    }
}
