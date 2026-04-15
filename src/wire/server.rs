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

use super::protocol::{MsgHeader, OpMsg, Section, MAX_MESSAGE_SIZE};
use crate::{
    client::{Client, ClientInner},
    error::Result,
    options::{
        FindOneAndDeleteOptions, FindOneAndUpdateOptions, FindOptions, InsertManyOptions,
        ReturnDocument, UpdateOptions,
    },
};

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

    /// Shared client inner state — used by CRUD command handlers.
    database: Arc<ClientInner>,
}

impl Default for ServerState {
    fn default() -> Self {
        let client = Client::open_in_memory().expect("in-memory client never fails");
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: None,
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
        }
    }
}

impl ServerState {
    /// Create state for a client at the given path (in-memory client for non-CRUD state).
    fn new(db_path: Option<std::path::PathBuf>) -> Self {
        let client = Client::open_in_memory().expect("in-memory client never fails");
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path,
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
        }
    }

    /// Create state backed by a real [`Client`] instance.
    ///
    /// Used by [`WireProtocol::bind`] to wire CRUD handlers to the actual client.
    fn new_with_db(client: &Client) -> Self {
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: client.inner.path.clone(),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
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
///
/// let client = Client::open_in_memory()?;
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
        let opcode = i32::from_le_bytes(header_buf[12..16].try_into().expect("slice is 4 bytes"));
        let request_id = i32::from_le_bytes(header_buf[4..8].try_into().expect("slice is 4 bytes"));

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
    let null_pos = after_flags.iter().position(|&b| b == 0).ok_or_else(|| {
        crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY fullCollectionName not null-terminated".into(),
        }
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
/// In the multi-database wire protocol (R2.1) any non-empty `$db` value is
/// accepted — the database is created on first write ("use mydb" semantics).
/// This function is retained for backward compatibility and always returns
/// `None` (i.e., no error).
#[allow(dead_code)]
fn check_db_field(_body: &Document, _server_db_name: &str) -> Option<Document> {
    None
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

    // In the multi-database wire protocol (R2.1) any database is accepted.
    // No Unauthorized check here — the fullCollectionName db prefix is used only
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
    // In the multi-database wire protocol (R2.1) any $db value is accepted.
    // No Unauthorized check — $db is used for routing, not access control.
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
    // Silently log (and ignore) fields that mqlite does not support.
    // Per integration.md: lsid, readConcern, writeConcern, $clusterTime, txnNumber
    // are silently ignored in Phase 1 — log at DEBUG, never return error.
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
        "hello" | "ismaster" => handle_hello(state, connection_id),
        "ping" => handle_ping(),
        "buildinfo" => handle_build_info(),
        "serverstatus" => handle_server_status(state),
        "listdatabases" => handle_list_databases(state),
        // CRUD commands
        "insert" => handle_insert(body, state),
        "find" => handle_find(body, state, cursors),
        "update" => handle_update(body, state),
        "delete" => handle_delete(body, state),
        "findandmodify" => handle_find_and_modify(body, state),
        // Cursor management
        "getmore" => handle_get_more(body, state, cursors),
        "killcursors" => handle_kill_cursors(body, cursors),
        // Collection admin
        "create" => handle_create(body, state),
        "drop" => handle_drop(body, state),
        "listcollections" => handle_list_collections(body, state),
        // Index operations
        "createindexes" => handle_create_indexes(body, state),
        "dropindexes" => handle_drop_indexes(body, state),
        "listindexes" => handle_list_indexes(body, state),
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
/// Enumerates all unique database namespaces that have at least one collection.
/// The database names are the prefixes of the `"db.collection"` keys stored
/// in the engine.  An empty mqlite instance (no writes yet) returns an empty
/// list, matching the MongoDB 8.0 behaviour where databases only appear after
/// the first write (`use mydb` semantics).
fn handle_list_databases(state: &ServerState) -> Document {
    // Collect unique database names from all "db.collection" collection names.
    let all_names = state.database.list_collection_names().unwrap_or_default();
    let mut db_set: std::collections::BTreeSet<String> = all_names
        .into_iter()
        .filter_map(|n| n.split('.').next().map(|db| db.to_owned()))
        .collect();
    // Remove internal engine namespaces that are not user databases.
    db_set.remove("$");

    let size_on_disk = state.wal_file_size() as i64;

    let databases: bson::Array = db_set
        .into_iter()
        .map(|name| {
            bson::Bson::Document(doc! {
                "name": &name,
                "sizeOnDisk": size_on_disk,
                "empty": false,
            })
        })
        .collect();

    doc! {
        "databases": databases,
        "totalSize": size_on_disk,
        "totalSizeMb": bson::Bson::Int64(size_on_disk / (1024 * 1024)),
        "ok": 1.0_f64,
    }
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
                merged.insert(
                    identifier.clone(),
                    bson::Bson::Array(
                        documents
                            .iter()
                            .map(|d| bson::Bson::Document(d.clone()))
                            .collect(),
                    ),
                );
            }
        }
    }
    merged
}

/// Extract an integer value from a BSON document field, coercing `Int32`,
/// `Int64`, and `Double` variants to `i64`.
fn get_i64(doc: &Document, key: &str) -> Option<i64> {
    match doc.get(key) {
        Some(bson::Bson::Int32(i)) => Some(*i as i64),
        Some(bson::Bson::Int64(i)) => Some(*i),
        Some(bson::Bson::Double(f)) => Some(*f as i64),
        _ => None,
    }
}

/// Build a `BadValue` (code 2) error response document.
fn err_bad_value(msg: impl Into<String>) -> Document {
    doc! {
        "ok": 0.0_f64,
        "errmsg": msg.into(),
        "code": crate::error::codes::BAD_VALUE,
        "codeName": "BadValue",
    }
}

/// Build a `collation not supported` error response (code 2, BadValue).
fn err_collation_unsupported() -> Document {
    err_bad_value("collation is not supported in this version of mqlite")
}

/// Convert a mqlite `Error` into a top-level command error document.
fn err_from_mqlite(e: crate::error::Error) -> Document {
    let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
    doc! {
        "ok": 0.0_f64,
        "errmsg": e.to_string(),
        "code": code,
        "codeName": mqlite_code_name(code),
    }
}

/// Map a MongoDB error code to its canonical `codeName` string.
fn mqlite_code_name(code: i32) -> &'static str {
    match code {
        crate::error::codes::DUPLICATE_KEY => "DuplicateKey",
        crate::error::codes::NAMESPACE_NOT_FOUND => "NamespaceNotFound",
        crate::error::codes::CURSOR_NOT_FOUND => "CursorNotFound",
        crate::error::codes::BAD_VALUE => "BadValue",
        crate::error::codes::UNSUPPORTED_OPERATOR => "FailedToParse",
        crate::error::codes::CANNOT_CREATE_INDEX => "CannotCreateIndex",
        _ => "InternalError",
    }
}

/// Convert a mqlite `Error` to a write-error `(code, message)` pair for
/// embedding inside a `writeErrors` array.
fn write_err_from_mqlite(e: &crate::error::Error) -> (i32, String) {
    let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
    (code, e.to_string())
}

// ---------------------------------------------------------------------------
// CRUD command handlers
// ---------------------------------------------------------------------------

/// `insert` — insert one or more documents.
///
/// Accepts documents from either `body["documents"]` (Kind-0) or a Kind-1
/// `"documents"` section (pymongo bulk path); see [`merge_doc_sequences_into_body`].
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <count>, "writeErrors": [...], "ok": 1.0 }
/// ```
fn handle_insert(body: &Document, state: &ServerState) -> Document {
    if body.contains_key("collation") {
        return err_collation_unsupported();
    }

    let coll_name = match body.get_str("insert") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("insert requires a collection name string"),
    };

    // Documents may arrive via Kind-1 merge or body array.
    let docs: Vec<Document> = match body.get_array("documents") {
        Ok(arr) => arr
            .iter()
            .filter_map(|b| b.as_document().cloned())
            .collect(),
        Err(_) => return err_bad_value("insert requires a \"documents\" array"),
    };

    if docs.is_empty() {
        // MongoDB allows empty inserts; return n=0 with ok:1.
        return doc! { "n": 0i32, "ok": 1.0_f64 };
    }

    let ordered = body.get_bool("ordered").unwrap_or(true);
    let opts = InsertManyOptions { ordered };

    match state
        .database
        .insert_many(&qualified_coll(body, &coll_name), &docs, opts)
    {
        Ok(result) => {
            let n = result.inserted_ids.len() as i32;
            if result.errors.is_empty() {
                doc! { "n": n, "ok": 1.0_f64 }
            } else {
                let write_errors: bson::Array = result
                    .errors
                    .iter()
                    .map(|e| {
                        bson::Bson::Document(doc! {
                            "index": e.index as i32,
                            "code": e.code,
                            "errmsg": &e.message,
                        })
                    })
                    .collect();
                doc! {
                    "n": n,
                    "writeErrors": write_errors,
                    "ok": 1.0_f64,
                }
            }
        }
        Err(e) => err_from_mqlite(e),
    }
}

/// `find` — query documents with filter, sort, projection, limit, skip.
///
/// Returns a cursor response with `firstBatch` and a server-side cursor ID
/// (non-zero when there are more results than the requested `batchSize`).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [...], "id": <cursor_id>, "ns": "db.coll" }, "ok": 1.0 }
/// ```
fn handle_find(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<std::sync::Mutex<ConnectionCursors>>,
) -> Document {
    if body.contains_key("collation") {
        return err_collation_unsupported();
    }

    let coll_name = match body.get_str("find") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("find requires a collection name string"),
    };

    let filter = body.get_document("filter").cloned().unwrap_or_default();

    let mut opts = FindOptions::new();
    if let Ok(sort) = body.get_document("sort") {
        opts.sort = Some(sort.clone());
    }
    if let Ok(proj) = body.get_document("projection") {
        opts.projection = Some(proj.clone());
    }
    if let Some(limit) = get_i64(body, "limit") {
        opts.limit = Some(limit);
    }
    if let Some(skip) = get_i64(body, "skip") {
        opts.skip = Some(skip as u64);
    }

    // Default first-batch size mirrors MongoDB 8.0 (101 documents).
    let batch_size = get_i64(body, "batchSize")
        .map(|n| if n <= 0 { 101usize } else { n as usize })
        .unwrap_or(101);

    let cursor =
        match state
            .database
            .find::<Document>(&qualified_coll(body, &coll_name), filter, opts)
        {
            Ok(c) => c,
            Err(e) => return err_from_mqlite(e),
        };

    // Collect all matching documents (cursor is already fully buffered in
    // memory by the storage engine, so this is a cheap move operation).
    let mut all_docs: Vec<Document> = Vec::new();
    for result in cursor {
        match result {
            Ok(d) => all_docs.push(d),
            Err(e) => return err_from_mqlite(e),
        }
    }

    let split_at = batch_size.min(all_docs.len());
    let remaining: Vec<Document> = all_docs.drain(split_at..).collect();
    let first_batch: bson::Array = all_docs
        .iter()
        .map(|d| bson::Bson::Document(d.clone()))
        .collect();

    // Store a server-side cursor for the remaining documents if any.
    let cursor_id: i64 = if remaining.is_empty() {
        0
    } else {
        let remaining_cursor = crate::Cursor::<Document>::new(remaining, 0);
        cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .store(remaining_cursor)
    };

    let ns = format!("{}.{}", extract_db_name(body), coll_name);
    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(cursor_id),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

/// `update` — update matching documents.
///
/// Processes the `updates` array; each entry may set `multi` and `upsert`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <matched>, "nModified": <modified>, "upserted": [...], "ok": 1.0 }
/// ```
fn handle_update(body: &Document, state: &ServerState) -> Document {
    if body.contains_key("collation") {
        return err_collation_unsupported();
    }

    let coll_name = match body.get_str("update") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("update requires a collection name string"),
    };

    let updates = match body.get_array("updates") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("update requires an \"updates\" array"),
    };

    let mut total_matched: i64 = 0;
    let mut total_modified: i64 = 0;
    let mut upserted: bson::Array = Vec::new();
    let mut write_errors: bson::Array = Vec::new();

    for (i, spec_bson) in updates.iter().enumerate() {
        let spec = match spec_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        // Per-spec collation check.
        if spec.contains_key("collation") {
            return err_collation_unsupported();
        }

        let filter = spec.get_document("q").cloned().unwrap_or_default();
        let update_doc = match spec.get_document("u") {
            Ok(u) => u.clone(),
            Err(_) => {
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": crate::error::codes::BAD_VALUE,
                    "errmsg": "update spec missing required \"u\" field",
                }));
                continue;
            }
        };
        let multi = spec.get_bool("multi").unwrap_or(false);
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        let opts = UpdateOptions { upsert };

        let result = if multi {
            state
                .database
                .update_many(&qualified_coll(body, &coll_name), filter, update_doc, opts)
        } else {
            state
                .database
                .update_one(&qualified_coll(body, &coll_name), filter, update_doc, opts)
        };

        match result {
            Ok(r) => {
                total_matched += r.matched_count as i64;
                total_modified += r.modified_count as i64;
                if let Some(id) = r.upserted_id {
                    upserted.push(bson::Bson::Document(doc! {
                        "index": i as i32,
                        "_id": id,
                    }));
                }
            }
            Err(e) => {
                let (code, msg) = write_err_from_mqlite(&e);
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": code,
                    "errmsg": msg,
                }));
            }
        }
    }

    let mut response = doc! {
        "n": bson::Bson::Int64(total_matched),
        "nModified": bson::Bson::Int64(total_modified),
        "ok": 1.0_f64,
    };
    if !upserted.is_empty() {
        response.insert("upserted", bson::Bson::Array(upserted));
    }
    if !write_errors.is_empty() {
        response.insert("writeErrors", bson::Bson::Array(write_errors));
    }
    response
}

/// `delete` — delete matching documents.
///
/// Processes the `deletes` array; `limit: 1` means deleteOne, `limit: 0` means
/// deleteMany (matching MongoDB wire protocol semantics).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <deleted>, "ok": 1.0 }
/// ```
fn handle_delete(body: &Document, state: &ServerState) -> Document {
    if body.contains_key("collation") {
        return err_collation_unsupported();
    }

    let coll_name = match body.get_str("delete") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("delete requires a collection name string"),
    };

    let deletes = match body.get_array("deletes") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("delete requires a \"deletes\" array"),
    };

    let mut total_deleted: i64 = 0;
    let mut write_errors: bson::Array = Vec::new();

    for (i, spec_bson) in deletes.iter().enumerate() {
        let spec = match spec_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        // Per-spec collation check.
        if spec.contains_key("collation") {
            return err_collation_unsupported();
        }

        let filter = spec.get_document("q").cloned().unwrap_or_default();
        // `limit: 0` = delete all matching; `limit: 1` (or any non-zero) = delete one.
        let limit = get_i64(spec, "limit").unwrap_or(1);

        let result = if limit == 0 {
            state
                .database
                .delete_many(&qualified_coll(body, &coll_name), filter)
        } else {
            state
                .database
                .delete_one(&qualified_coll(body, &coll_name), filter)
        };

        match result {
            Ok(r) => total_deleted += r.deleted_count as i64,
            Err(e) => {
                let (code, msg) = write_err_from_mqlite(&e);
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": code,
                    "errmsg": msg,
                }));
            }
        }
    }

    let mut response = doc! {
        "n": bson::Bson::Int64(total_deleted),
        "ok": 1.0_f64,
    };
    if !write_errors.is_empty() {
        response.insert("writeErrors", bson::Bson::Array(write_errors));
    }
    response
}

/// `findAndModify` — atomically find and modify (update or remove) a document.
///
/// The response uses the `value` field (not `document`) as required by
/// MongoDB 8.0 wire protocol semantics.
///
/// Response format (MongoDB 8.0):
/// ```json
/// {
///   "value": <doc_or_null>,
///   "lastErrorObject": { "n": 1, "updatedExisting": true },
///   "ok": 1.0
/// }
/// ```
fn handle_find_and_modify(body: &Document, state: &ServerState) -> Document {
    if body.contains_key("collation") {
        return err_collation_unsupported();
    }

    // Command key can be either "findAndModify" or "findandmodify" (case-insensitive
    // dispatch normalises to lowercase in route_command).
    let coll_name = body
        .get_str("findandmodify")
        .or_else(|_| body.get_str("findAndModify"))
        .map(|s| s.to_owned())
        .unwrap_or_default();
    if coll_name.is_empty() {
        return err_bad_value("findAndModify requires a collection name string");
    }

    let filter = body.get_document("query").cloned().unwrap_or_default();
    let remove = body.get_bool("remove").unwrap_or(false);
    let return_new = body.get_bool("new").unwrap_or(false);
    let upsert = body.get_bool("upsert").unwrap_or(false);
    let sort = body.get_document("sort").ok().cloned();

    if remove {
        // ---- findAndModify + remove ----
        let opts = FindOneAndDeleteOptions { sort };
        match state.database.find_one_and_delete_with_options::<Document>(
            &qualified_coll(body, &coll_name),
            filter,
            opts,
        ) {
            Ok(Some(doc)) => doc! {
                "value": bson::Bson::Document(doc),
                "lastErrorObject": { "n": 1i32 },
                "ok": 1.0_f64,
            },
            Ok(None) => doc! {
                "value": bson::Bson::Null,
                "lastErrorObject": { "n": 0i32 },
                "ok": 1.0_f64,
            },
            Err(e) => err_from_mqlite(e),
        }
    } else {
        // ---- findAndModify + update ----
        let update_doc = match body.get_document("update") {
            Ok(u) => u.clone(),
            Err(_) => {
                return err_bad_value("findAndModify requires either \"update\" or \"remove\"")
            }
        };

        let return_document = if return_new {
            ReturnDocument::After
        } else {
            ReturnDocument::Before
        };
        let opts = FindOneAndUpdateOptions {
            return_document,
            upsert,
            sort,
        };

        match state.database.find_one_and_update_with_options::<Document>(
            &qualified_coll(body, &coll_name),
            filter,
            update_doc,
            opts,
        ) {
            Ok(Some(doc)) => {
                // A document was returned.
                // With ReturnDocument::Before this is the original (updatedExisting=true).
                // With ReturnDocument::After this is the post-update doc; we cannot
                // distinguish update-of-existing vs upsert from the return value alone,
                // so we conservatively report updatedExisting=true (the common path).
                let updated_existing = true;
                doc! {
                    "value": bson::Bson::Document(doc),
                    "lastErrorObject": {
                        "n": 1i32,
                        "updatedExisting": updated_existing,
                    },
                    "ok": 1.0_f64,
                }
            }
            Ok(None) => {
                // No document found (or upsert with ReturnDocument::Before).
                doc! {
                    "value": bson::Bson::Null,
                    "lastErrorObject": {
                        "n": if upsert { 1i32 } else { 0i32 },
                        "updatedExisting": false,
                    },
                    "ok": 1.0_f64,
                }
            }
            Err(e) => err_from_mqlite(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Cursor management command handlers
// ---------------------------------------------------------------------------

/// `getMore` — fetch the next batch of results from an open server-side cursor.
///
/// Cursors are pinned to the TCP connection that created them via `find`.  A
/// `getMore` sent on a *different* connection will always get `CursorNotFound`
/// (code 43) because the cursor map is per-connection.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "nextBatch": [...], "id": <cursor_id_or_0>, "ns": "db.coll" }, "ok": 1 }
/// ```
/// When the cursor is exhausted the response contains `"id": 0` and the cursor
/// is removed from the per-connection map.
fn handle_get_more(
    body: &Document,
    _state: &ServerState,
    cursors: &Arc<std::sync::Mutex<ConnectionCursors>>,
) -> Document {
    let cursor_id = match get_i64(body, "getMore") {
        Some(id) => id,
        None => return err_bad_value("getMore requires a cursor id (Int64)"),
    };

    let coll_name = match body.get_str("collection") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("getMore requires a \"collection\" field"),
    };

    // Default batch size mirrors MongoDB 8.0 (101 documents).
    let batch_size = get_i64(body, "batchSize")
        .map(|n| if n <= 0 { 101usize } else { n as usize })
        .unwrap_or(101);

    // Drain up to `batch_size` documents from the cursor in a single critical section.
    let (next_batch, exhausted) = {
        let mut guard = cursors.lock().unwrap_or_else(|e| e.into_inner());
        match guard.get_mut(cursor_id) {
            None => {
                return doc! {
                    "ok": 0.0_f64,
                    "errmsg": format!("cursor id {} not found", cursor_id),
                    "code": crate::error::codes::CURSOR_NOT_FOUND,
                    "codeName": "CursorNotFound",
                };
            }
            Some(cursor) => {
                let batch: bson::Array = cursor
                    .by_ref()
                    .take(batch_size)
                    .filter_map(|r| r.ok().map(bson::Bson::Document))
                    .collect();
                let done = cursor.is_exhausted();
                (batch, done)
            }
        }
    };

    // Remove the cursor from the map once it is exhausted.
    let returned_id: i64 = if exhausted {
        cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(cursor_id);
        0
    } else {
        cursor_id
    };

    let ns = format!("{}.{}", extract_db_name(body), coll_name);
    doc! {
        "cursor": {
            "nextBatch": next_batch,
            "id": bson::Bson::Int64(returned_id),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

/// `killCursors` — close one or more open server-side cursors and release resources.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursorsKilled": [...], "cursorsNotFound": [...], "ok": 1 }
/// ```
fn handle_kill_cursors(
    body: &Document,
    cursors: &Arc<std::sync::Mutex<ConnectionCursors>>,
) -> Document {
    let cursor_ids: Vec<i64> = match body.get_array("cursors") {
        Ok(arr) => arr
            .iter()
            .filter_map(|b| match b {
                bson::Bson::Int64(id) => Some(*id),
                bson::Bson::Int32(id) => Some(*id as i64),
                _ => None,
            })
            .collect(),
        Err(_) => return err_bad_value("killCursors requires a \"cursors\" array"),
    };

    let mut killed: bson::Array = Vec::new();
    let mut not_found: bson::Array = Vec::new();

    let mut guard = cursors.lock().unwrap_or_else(|e| e.into_inner());
    for id in &cursor_ids {
        if guard.remove(*id).is_some() {
            killed.push(bson::Bson::Int64(*id));
        } else {
            not_found.push(bson::Bson::Int64(*id));
        }
    }

    doc! {
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "ok": 1.0_f64,
    }
}

// ---------------------------------------------------------------------------
// Collection admin command handlers
// ---------------------------------------------------------------------------

/// `create` — explicitly create a collection.
///
/// This is idempotent: creating an already-existing collection returns `{ok: 1}`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
fn handle_create(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("create") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("create requires a collection name string"),
    };

    match state
        .database
        .create_collection(&qualified_coll(body, &coll_name))
    {
        Ok(_) => doc! { "ok": 1.0_f64 },
        Err(e) => err_from_mqlite(e),
    }
}

/// `drop` — drop a collection and all its indexes.
///
/// Dropping a non-existent collection returns `{ok: 1}` (idempotent, matching
/// MongoDB 8.0 behaviour for `drop` on a missing namespace).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
fn handle_drop(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("drop") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("drop requires a collection name string"),
    };

    match state
        .database
        .drop_collection(&qualified_coll(body, &coll_name))
    {
        Ok(_) => doc! { "ok": 1.0_f64 },
        Err(e) => err_from_mqlite(e),
    }
}

/// `listCollections` — list collections in the current database.
///
/// Supports an optional `filter` document with a `name` equality filter.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [{name, type, options, idIndex, info}], "id": 0 }, "ok": 1 }
/// ```
fn handle_list_collections(body: &Document, state: &ServerState) -> Document {
    // Optional `filter: {name: "<name>"}` — only a simple equality filter on `name`
    // is supported in Phase 1.
    let name_filter: Option<String> = body
        .get_document("filter")
        .ok()
        .and_then(|f| f.get_str("name").ok())
        .map(|s| s.to_owned());

    let all_names = match state.database.list_collection_names() {
        Ok(n) => n,
        Err(e) => return err_from_mqlite(e),
    };

    // Filter to collections in the database named by `$db`.
    let db_name = extract_db_name(body);
    let db_prefix = format!("{db_name}.");
    let names: Vec<String> = all_names
        .into_iter()
        .filter_map(|n| {
            // Names are stored as "db.collection" — strip the db prefix.
            n.strip_prefix(&db_prefix).map(|s| s.to_owned())
        })
        .collect();

    let first_batch: bson::Array = names
        .into_iter()
        .filter(|name| name_filter.as_ref().map_or(true, |filter| name == filter))
        .map(|name| {
            bson::Bson::Document(doc! {
                "name": &name,
                "type": "collection",
                "options": {},
                "idIndex": {
                    "v": 2i32,
                    "key": {"_id": 1i32},
                    "name": "_id_",
                },
                "info": {
                    "readOnly": false,
                },
            })
        })
        .collect();

    // The cursor namespace for listCollections uses `$cmd.listCollections`.
    let ns = format!("{}.$cmd.listCollections", db_name);
    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(0i64),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

// ---------------------------------------------------------------------------
// Index operation command handlers
// ---------------------------------------------------------------------------

/// `createIndexes` — create one or more indexes on a collection.
///
/// Each index specification in `indexes` must contain at minimum a `key`
/// document.  Optionally: `name`, `unique`, `sparse`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "numIndexesBefore": <n>, "numIndexesAfter": <n>, "ok": 1 }
/// ```
/// `numIndexesBefore` and `numIndexesAfter` both include the synthetic `_id_`
/// index (always present in every MongoDB collection).
fn handle_create_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("createIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("createIndexes requires a collection name string"),
    };

    let indexes_arr = match body.get_array("indexes") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("createIndexes requires an \"indexes\" array"),
    };

    // Count existing user-created indexes before creation.
    // Add 1 for the always-present synthetic `_id_` index.
    let num_before = state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
        .map(|idxs| idxs.len() as i32 + 1)
        .unwrap_or(1);

    for idx_bson in &indexes_arr {
        let spec = match idx_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        let key = match spec.get_document("key") {
            Ok(k) => k.clone(),
            Err(_) => return err_bad_value("each index spec requires a \"key\" document"),
        };

        let mut opts = crate::options::IndexOptions::new();
        if let Ok(b) = spec.get_bool("unique") {
            opts = opts.unique(b);
        }
        if let Ok(b) = spec.get_bool("sparse") {
            opts = opts.sparse(b);
        }
        if let Ok(name) = spec.get_str("name") {
            opts = opts.name(name);
        }

        let model = crate::index::IndexModel {
            keys: key,
            options: opts,
        };
        if let Err(e) = state
            .database
            .create_index(&qualified_coll(body, &coll_name), model)
        {
            return err_from_mqlite(e);
        }
    }

    // Count user-created indexes after creation (+1 for synthetic `_id_`).
    let num_after = state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
        .map(|idxs| idxs.len() as i32 + 1)
        .unwrap_or(1);

    doc! {
        "numIndexesBefore": num_before,
        "numIndexesAfter": num_after,
        "ok": 1.0_f64,
    }
}

/// `dropIndexes` — drop one or all user-created indexes on a collection.
///
/// The `index` field may be:
/// - `"*"` — drop all user-created indexes (the `_id_` index is never dropped).
/// - `"<name>"` — drop the named index.
/// - `{<key pattern>}` — drop the index with the matching key pattern.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
fn handle_drop_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("dropIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("dropIndexes requires a collection name string"),
    };

    match body.get("index") {
        Some(bson::Bson::String(name)) if name == "*" => {
            // Drop all user-created indexes.
            let indexes = match state
                .database
                .list_indexes(&qualified_coll(body, &coll_name))
            {
                Ok(idxs) => idxs,
                Err(e) => return err_from_mqlite(e),
            };
            for idx in &indexes {
                if let Err(e) = state
                    .database
                    .drop_index(&qualified_coll(body, &coll_name), &idx.name)
                {
                    return err_from_mqlite(e);
                }
            }
            doc! { "ok": 1.0_f64 }
        }
        Some(bson::Bson::String(name)) => {
            // Drop a specific index by name.
            match state
                .database
                .drop_index(&qualified_coll(body, &coll_name), name)
            {
                Ok(_) => doc! { "ok": 1.0_f64 },
                Err(e) => err_from_mqlite(e),
            }
        }
        Some(bson::Bson::Document(key_doc)) => {
            // Drop by key pattern — find the index whose key matches.
            let key_doc = key_doc.clone();
            let indexes = match state
                .database
                .list_indexes(&qualified_coll(body, &coll_name))
            {
                Ok(idxs) => idxs,
                Err(e) => return err_from_mqlite(e),
            };
            match indexes.iter().find(|idx| idx.keys == key_doc) {
                Some(idx) => match state
                    .database
                    .drop_index(&qualified_coll(body, &coll_name), &idx.name.clone())
                {
                    Ok(_) => doc! { "ok": 1.0_f64 },
                    Err(e) => err_from_mqlite(e),
                },
                None => doc! {
                    "ok": 0.0_f64,
                    "errmsg": "index not found with name",
                    "code": 27i32,
                    "codeName": "IndexNotFound",
                },
            }
        }
        _ => err_bad_value(
            "dropIndexes requires an \"index\" field (string name, \"*\", or key document)",
        ),
    }
}

/// `listIndexes` — list indexes on a collection.
///
/// Always returns the synthetic `_id_` index first (MongoDB always reports it),
/// followed by any user-created indexes.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [{v, key, name, ...}], "id": 0 }, "ok": 1 }
/// ```
fn handle_list_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("listIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("listIndexes requires a collection name string"),
    };

    let indexes = match state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
    {
        Ok(idxs) => idxs,
        Err(e) => return err_from_mqlite(e),
    };

    // The `_id_` index is always present in every MongoDB collection.
    let mut first_batch: bson::Array = vec![bson::Bson::Document(doc! {
        "v": 2i32,
        "key": {"_id": 1i32},
        "name": "_id_",
    })];

    for idx in &indexes {
        let mut idx_doc = doc! {
            "v": 2i32,
            "key": idx.keys.clone(),
            "name": &idx.name,
        };
        if idx.unique {
            idx_doc.insert("unique", true);
        }
        if idx.sparse {
            idx_doc.insert("sparse", true);
        }
        first_batch.push(bson::Bson::Document(idx_doc));
    }

    let ns = format!("{}.{}", extract_db_name(body), coll_name);
    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(0i64),
            "ns": ns,
        },
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

    /// Return an empty per-connection cursor map for use in unit tests that
    /// do not exercise cursor-related functionality.
    fn dummy_cursors() -> Arc<std::sync::Mutex<ConnectionCursors>> {
        Arc::new(std::sync::Mutex::new(ConnectionCursors::new()))
    }

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
        let resp_bytes =
            dispatch_op_msg(&msg, 10, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn dispatch_op_msg_hello() {
        let state = ServerState::default();
        let req_buf = make_op_msg_request(2, &doc! { "hello": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 11, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
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
        let req_buf =
            make_op_query_request(3, "admin.$cmd", &doc! { "ismaster": 1, "helloOk": true });
        let resp_bytes = dispatch_op_query(&req_buf, 12, 3, &state, 2).unwrap();

        // Response must be OP_REPLY (opcode 1).
        let header = MsgHeader::parse(&resp_bytes).unwrap();
        assert_eq!(header.op_code, OP_REPLY);
        assert_eq!(header.response_to, 3);

        // Parse the OP_REPLY body.
        // Layout: header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
        let doc_start = 16 + 4 + 8 + 4 + 4;
        let doc_size =
            i32::from_le_bytes(resp_bytes[doc_start..doc_start + 4].try_into().unwrap()) as usize;
        let raw =
            bson::RawDocumentBuf::from_bytes(resp_bytes[doc_start..doc_start + doc_size].to_vec())
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
        let resp_bytes =
            dispatch_op_msg(&msg, 12, msg.header.request_id, &state, 3, &dummy_cursors()).unwrap();
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
        let resp_bytes =
            dispatch_op_msg(&msg, 13, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
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
        let resp_bytes =
            dispatch_op_msg(&msg, 14, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
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
        // Insert a document so the database is visible in listDatabases (R2.1).
        let state = ServerState::default();
        let cursors = dummy_cursors();
        let ins_req = make_op_msg_request(
            5,
            &doc! { "insert": "col", "documents": [{"x": 1i32}], "$db": "testdb" },
        );
        let ins_msg = OpMsg::parse(&ins_req).unwrap();
        dispatch_op_msg(&ins_msg, 14, ins_msg.header.request_id, &state, 1, &cursors).unwrap();

        let req_buf = make_op_msg_request(6, &doc! { "listDatabases": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 15, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        // After the insert, "testdb" must appear.
        let dbs = body.get_array("databases").unwrap();
        assert!(
            !dbs.is_empty(),
            "at least one database must appear after insert"
        );
        let names: Vec<&str> = dbs
            .iter()
            .map(|d| d.as_document().unwrap().get_str("name").unwrap())
            .collect();
        assert!(
            names.contains(&"testdb"),
            "testdb must appear in listDatabases"
        );
    }

    #[test]
    fn dispatch_op_msg_unknown_command() {
        let state = ServerState::default();
        // Use $db: "admin" (always allowed) to test CommandNotFound, not Unauthorized.
        let req_buf = make_op_msg_request(7, &doc! { "aggregate": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 16, msg.header.request_id, &state, 1, &dummy_cursors()).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 0.0);
        assert_eq!(body.get_i32("code").unwrap(), 59);
        assert_eq!(body.get_str("codeName").unwrap(), "CommandNotFound");
    }

    // -----------------------------------------------------------------------
    // $db field routing (R2.1 multi-database — any $db is accepted)
    // -----------------------------------------------------------------------

    #[test]
    fn check_db_field_always_returns_none() {
        // Multi-database: any $db value is accepted — check_db_field is a no-op.
        assert!(check_db_field(&doc! { "ping": 1 }, "myapp").is_none());
        assert!(check_db_field(&doc! { "ping": 1, "$db": "admin" }, "myapp").is_none());
        assert!(check_db_field(&doc! { "ping": 1, "$db": "myapp" }, "myapp").is_none());
        assert!(
            check_db_field(&doc! { "ping": 1, "$db": "wrongdb" }, "myapp").is_none(),
            "any $db must be accepted in multi-database mode"
        );
    }

    #[test]
    fn dispatch_op_msg_any_db_is_allowed() {
        // R2.1: arbitrary $db values must succeed (no Unauthorized for unknown db).
        let state = ServerState::default();
        for db in &["admin", "local", "mydb", "arbitrarydb", "test"] {
            let req_buf = make_op_msg_request(20, &doc! { "ping": 1, "$db": db });
            let msg = OpMsg::parse(&req_buf).unwrap();
            let resp_bytes =
                dispatch_op_msg(&msg, 40, msg.header.request_id, &state, 1, &dummy_cursors())
                    .unwrap();
            let resp = OpMsg::parse(&resp_bytes).unwrap();
            let body = resp.body().unwrap();
            assert_eq!(
                body.get_f64("ok").unwrap(),
                1.0,
                "$db='{}' should succeed but got: {:?}",
                db,
                body
            );
        }
    }

    #[test]
    fn dispatch_op_msg_db_routes_to_correct_namespace() {
        // Documents inserted with $db: "foo" must not appear in $db: "bar".
        let state = ServerState::default();
        let cursors = dummy_cursors();
        handle_insert(
            &doc! { "insert": "things", "documents": [{"x": 1i32}], "$db": "foo" },
            &state,
        );
        // find in same db — must return 1 doc.
        let find_foo = handle_find(
            &doc! { "find": "things", "filter": {}, "$db": "foo" },
            &state,
            &cursors,
        );
        let batch_foo = find_foo
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            batch_foo.len(),
            1,
            "find in same db must return the document"
        );

        // find in different db — must return 0 docs.
        let find_bar = handle_find(
            &doc! { "find": "things", "filter": {}, "$db": "bar" },
            &state,
            &cursors,
        );
        let batch_bar = find_bar
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert!(
            batch_bar.is_empty(),
            "find in different db must return no documents"
        );
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
    fn dispatch_op_query_any_db_is_allowed() {
        // R2.1: OP_QUERY from any database must succeed (isMaster handshake).
        let state = ServerState::default();
        let req_buf = make_op_query_request(30, "anydb.$cmd", &doc! { "ismaster": 1 });
        let resp_bytes = dispatch_op_query(&req_buf, 50, 30, &state, 1).unwrap();
        // Parse OP_REPLY body.
        let doc_start = 16 + 4 + 8 + 4 + 4;
        let doc_size =
            i32::from_le_bytes(resp_bytes[doc_start..doc_start + 4].try_into().unwrap()) as usize;
        let raw =
            bson::RawDocumentBuf::from_bytes(resp_bytes[doc_start..doc_start + doc_size].to_vec())
                .unwrap();
        let body = bson::from_slice::<Document>(raw.as_bytes()).unwrap();
        // Must succeed — any $db is valid in multi-database mode.
        assert_eq!(
            body.get_f64("ok").unwrap(),
            1.0,
            "OP_QUERY from any db must succeed, got: {:?}",
            body
        );
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
        assert_eq!(
            pid1, pid2,
            "topology processId should be stable across calls"
        );
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
    // listDatabases — spec compliance (R2.1 multi-database)
    // -----------------------------------------------------------------------

    #[test]
    fn list_databases_empty_when_no_collections() {
        // Empty server — no collections yet — must report no databases.
        let state = ServerState::default();
        let body = handle_list_databases(&state);
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        let dbs = body.get_array("databases").unwrap();
        assert!(dbs.is_empty(), "empty server must report no databases");
    }

    #[test]
    fn list_databases_shows_db_after_insert() {
        // After inserting into "mydb", listDatabases must include "mydb".
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "col", "documents": [{"x": 1i32}], "$db": "mydb" },
            &state,
        );
        let body = handle_list_databases(&state);
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        let dbs = body.get_array("databases").unwrap();
        assert_eq!(dbs.len(), 1);
        let db_doc = dbs[0].as_document().unwrap();
        assert_eq!(db_doc.get_str("name").unwrap(), "mydb");
        assert!(
            db_doc.contains_key("sizeOnDisk"),
            "database entry must have sizeOnDisk"
        );
        assert!(
            db_doc.contains_key("empty"),
            "database entry must have empty"
        );
    }

    #[test]
    fn list_databases_multiple_databases() {
        // Multiple $db namespaces are each reported as a separate database.
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "a", "documents": [{"x": 1i32}], "$db": "alpha" },
            &state,
        );
        handle_insert(
            &doc! { "insert": "b", "documents": [{"y": 2i32}], "$db": "beta" },
            &state,
        );
        let body = handle_list_databases(&state);
        let dbs = body.get_array("databases").unwrap();
        assert_eq!(dbs.len(), 2);
        let names: Vec<&str> = dbs
            .iter()
            .map(|d| d.as_document().unwrap().get_str("name").unwrap())
            .collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
    }

    #[test]
    fn list_databases_same_db_different_collections_counted_once() {
        // Two collections in "shared" — should appear as one entry.
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "c1", "documents": [{"v": 1i32}], "$db": "shared" },
            &state,
        );
        handle_insert(
            &doc! { "insert": "c2", "documents": [{"v": 2i32}], "$db": "shared" },
            &state,
        );
        let body = handle_list_databases(&state);
        let dbs = body.get_array("databases").unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(
            dbs[0].as_document().unwrap().get_str("name").unwrap(),
            "shared"
        );
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

        let client = Client::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

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

        let client = Client::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

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
        let doc_size =
            i32::from_le_bytes(rest[doc_start..doc_start + 4].try_into().unwrap()) as usize;
        let raw = bson::RawDocumentBuf::from_bytes(rest[doc_start..doc_start + doc_size].to_vec())
            .unwrap();
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

        let client = Client::open_in_memory().unwrap();
        let _server = WireProtocol::bind(&client, &addr.to_string()).unwrap();

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

    // -----------------------------------------------------------------------
    // CRUD command handler unit tests
    // -----------------------------------------------------------------------

    // ---- insert ----

    #[test]
    fn insert_single_document_returns_n_1() {
        let state = ServerState::default();
        let body = doc! {
            "insert": "users",
            "documents": [{"name": "Alice", "age": 30i32}],
            "$db": "local",
        };
        let result = handle_insert(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        assert_eq!(result.get_i32("n").unwrap(), 1);
        assert!(!result.contains_key("writeErrors"));
    }

    #[test]
    fn insert_many_documents_ordered() {
        let state = ServerState::default();
        let body = doc! {
            "insert": "items",
            "documents": [
                {"x": 1i32},
                {"x": 2i32},
                {"x": 3i32},
            ],
            "ordered": true,
            "$db": "local",
        };
        let result = handle_insert(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(result.get_i32("n").unwrap(), 3);
    }

    #[test]
    fn insert_empty_documents_returns_n_0() {
        let state = ServerState::default();
        let body = doc! {
            "insert": "empty",
            "documents": [],
            "$db": "local",
        };
        let result = handle_insert(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(result.get_i32("n").unwrap(), 0);
    }

    #[test]
    fn insert_collation_returns_bad_value() {
        let state = ServerState::default();
        let body = doc! {
            "insert": "col",
            "documents": [],
            "collation": {"locale": "en"},
            "$db": "local",
        };
        let result = handle_insert(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 2); // BadValue
    }

    /// Insert via Kind-1 document sequence (pymongo bulk path).
    #[test]
    fn insert_via_doc_sequence_merged_into_body() {
        let state = ServerState::default();
        // Simulate what happens after merge_doc_sequences_into_body:
        // the Kind-1 "documents" section has been merged into the body.
        let body = doc! {
            "insert": "merged",
            "documents": [{"a": 1i32}, {"a": 2i32}],
            "$db": "local",
        };
        let result = handle_insert(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(result.get_i32("n").unwrap(), 2);
    }

    // ---- find ----

    #[test]
    fn find_empty_collection_returns_empty_first_batch() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        let body = doc! {
            "find": "nonexistent",
            "filter": {},
            "$db": "local",
        };
        let result = handle_find(&body, &state, &cursors);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        let cursor_doc = result.get_document("cursor").unwrap();
        let first_batch = cursor_doc.get_array("firstBatch").unwrap();
        assert!(
            first_batch.is_empty(),
            "empty collection must return firstBatch=[]"
        );
        assert_eq!(
            cursor_doc.get_i64("id").unwrap(),
            0,
            "cursor id must be 0 when exhausted"
        );
        assert!(cursor_doc.get_str("ns").is_ok(), "ns field must be present");
    }

    #[test]
    fn find_returns_inserted_documents() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        // Insert 3 docs first.
        let insert_body = doc! {
            "insert": "findtest",
            "documents": [{"v": 1i32}, {"v": 2i32}, {"v": 3i32}],
            "$db": "local",
        };
        let ins_res = handle_insert(&insert_body, &state);
        assert_eq!(ins_res.get_f64("ok").unwrap(), 1.0);

        let find_body = doc! {
            "find": "findtest",
            "filter": {},
            "$db": "local",
        };
        let result = handle_find(&find_body, &state, &cursors);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let cursor_doc = result.get_document("cursor").unwrap();
        let first_batch = cursor_doc.get_array("firstBatch").unwrap();
        assert_eq!(first_batch.len(), 3);
        // cursor exhausted — no server-side cursor needed
        assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
    }

    #[test]
    fn find_with_filter_returns_matching_docs() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        let insert_body = doc! {
            "insert": "filtercoll",
            "documents": [
                {"status": "active", "n": 1i32},
                {"status": "inactive", "n": 2i32},
                {"status": "active", "n": 3i32},
            ],
            "$db": "local",
        };
        handle_insert(&insert_body, &state);

        let find_body = doc! {
            "find": "filtercoll",
            "filter": {"status": "active"},
            "$db": "local",
        };
        let result = handle_find(&find_body, &state, &cursors);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let cursor_doc = result.get_document("cursor").unwrap();
        assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 2);
    }

    #[test]
    fn find_batch_size_creates_server_side_cursor() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        // Insert 5 documents.
        let insert_body = doc! {
            "insert": "batchcoll",
            "documents": [
                {"i": 0i32}, {"i": 1i32}, {"i": 2i32},
                {"i": 3i32}, {"i": 4i32},
            ],
            "$db": "local",
        };
        handle_insert(&insert_body, &state);

        // Request only 2 per batch.
        let find_body = doc! {
            "find": "batchcoll",
            "filter": {},
            "batchSize": 2i32,
            "$db": "local",
        };
        let result = handle_find(&find_body, &state, &cursors);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let cursor_doc = result.get_document("cursor").unwrap();
        let first_batch = cursor_doc.get_array("firstBatch").unwrap();
        assert_eq!(
            first_batch.len(),
            2,
            "firstBatch must have exactly batchSize docs"
        );
        let cursor_id = cursor_doc.get_i64("id").unwrap();
        assert_ne!(
            cursor_id, 0,
            "cursor id must be non-zero when more docs remain"
        );
        // The server-side cursor should be stored.
        assert_eq!(cursors.lock().unwrap().len(), 1);
    }

    #[test]
    fn find_collation_returns_bad_value() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        let body = doc! {
            "find": "col",
            "filter": {},
            "collation": {"locale": "en"},
            "$db": "local",
        };
        let result = handle_find(&body, &state, &cursors);
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 2);
    }

    // ---- update ----

    #[test]
    fn update_one_modifies_single_document() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        // Seed.
        handle_insert(
            &doc! { "insert": "updcoll", "documents": [{"k": "a", "v": 1i32}, {"k": "a", "v": 2i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "update": "updcoll",
            "updates": [{
                "q": {"k": "a"},
                "u": {"$set": {"v": 99i32}},
                "multi": false,
            }],
            "$db": "local",
        };
        let result = handle_update(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        assert_eq!(result.get_i64("n").unwrap(), 1);
        assert_eq!(result.get_i64("nModified").unwrap(), 1);

        // Verify only one was modified.
        let find_res = handle_find(
            &doc! { "find": "updcoll", "filter": {"v": 99i32}, "$db": "local" },
            &state,
            &cursors,
        );
        let batch = find_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn update_many_modifies_all_matching() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        handle_insert(
            &doc! { "insert": "multcoll", "documents": [{"x": 1i32}, {"x": 1i32}, {"x": 2i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "update": "multcoll",
            "updates": [{
                "q": {"x": 1i32},
                "u": {"$set": {"x": 10i32}},
                "multi": true,
            }],
            "$db": "local",
        };
        let result = handle_update(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(result.get_i64("n").unwrap(), 2);
        assert_eq!(result.get_i64("nModified").unwrap(), 2);
    }

    #[test]
    fn update_with_upsert_inserts_new_document() {
        let state = ServerState::default();
        let body = doc! {
            "update": "upsertcoll",
            "updates": [{
                "q": {"_id": "new-id"},
                "u": {"$set": {"created": true}},
                "upsert": true,
            }],
            "$db": "local",
        };
        let result = handle_update(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        // Upserted array must contain the new document id.
        let upserted = result.get_array("upserted").unwrap();
        assert_eq!(upserted.len(), 1);
        let upsert_entry = upserted[0].as_document().unwrap();
        assert_eq!(upsert_entry.get_i32("index").unwrap(), 0);
        assert!(upsert_entry.contains_key("_id"));
    }

    #[test]
    fn update_collation_returns_bad_value() {
        let state = ServerState::default();
        let body = doc! {
            "update": "col",
            "updates": [{"q": {}, "u": {"$set": {"x": 1i32}}}],
            "collation": {"locale": "en"},
            "$db": "local",
        };
        let result = handle_update(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 2);
    }

    // ---- delete ----

    #[test]
    fn delete_one_removes_single_document() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        handle_insert(
            &doc! { "insert": "delcoll", "documents": [{"k": 1i32}, {"k": 1i32}, {"k": 2i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "delete": "delcoll",
            "deletes": [{ "q": {"k": 1i32}, "limit": 1i32 }],
            "$db": "local",
        };
        let result = handle_delete(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        assert_eq!(result.get_i64("n").unwrap(), 1);

        // Two docs with k=1 were inserted; one remains.
        let find_res = handle_find(
            &doc! { "find": "delcoll", "filter": {"k": 1i32}, "$db": "local" },
            &state,
            &cursors,
        );
        let batch = find_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn delete_many_removes_all_matching() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        handle_insert(
            &doc! { "insert": "delmanycoll", "documents": [{"t": "x"}, {"t": "x"}, {"t": "y"}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "delete": "delmanycoll",
            "deletes": [{ "q": {"t": "x"}, "limit": 0i32 }],
            "$db": "local",
        };
        let result = handle_delete(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(result.get_i64("n").unwrap(), 2);

        // Only doc with t=y remains.
        let find_res = handle_find(
            &doc! { "find": "delmanycoll", "filter": {}, "$db": "local" },
            &state,
            &cursors,
        );
        let batch = find_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn delete_collation_returns_bad_value() {
        let state = ServerState::default();
        let body = doc! {
            "delete": "col",
            "deletes": [{"q": {}, "limit": 1i32}],
            "collation": {"locale": "en"},
            "$db": "local",
        };
        let result = handle_delete(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 2);
    }

    // ---- findAndModify ----

    #[test]
    fn find_and_modify_update_returns_original_doc() {
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "famcoll", "documents": [{"name": "Alice", "score": 10i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "findandmodify": "famcoll",
            "query": {"name": "Alice"},
            "update": {"$set": {"score": 99i32}},
            "new": false,
            "$db": "local",
        };
        let result = handle_find_and_modify(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        // Response must use 'value' not 'document'.
        assert!(
            result.contains_key("value"),
            "response must use 'value' field"
        );
        assert!(
            !result.contains_key("document"),
            "response must NOT use 'document' field"
        );
        let value = result.get_document("value").unwrap();
        assert_eq!(value.get_str("name").unwrap(), "Alice");
        // Original score before update.
        assert_eq!(value.get_i32("score").unwrap(), 10);
        let leo = result.get_document("lastErrorObject").unwrap();
        assert_eq!(leo.get_i32("n").unwrap(), 1);
    }

    #[test]
    fn find_and_modify_update_new_true_returns_updated_doc() {
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "famnewcoll", "documents": [{"v": 1i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "findandmodify": "famnewcoll",
            "query": {"v": 1i32},
            "update": {"$set": {"v": 2i32}},
            "new": true,
            "$db": "local",
        };
        let result = handle_find_and_modify(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let value = result.get_document("value").unwrap();
        assert_eq!(value.get_i32("v").unwrap(), 2); // post-update
    }

    #[test]
    fn find_and_modify_no_match_returns_null_value() {
        let state = ServerState::default();
        let body = doc! {
            "findandmodify": "emptyfamcoll",
            "query": {"nonexistent": true},
            "update": {"$set": {"x": 1i32}},
            "$db": "local",
        };
        let result = handle_find_and_modify(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert_eq!(
            result.get("value"),
            Some(&bson::Bson::Null),
            "value must be null when no doc matches"
        );
        let leo = result.get_document("lastErrorObject").unwrap();
        assert_eq!(leo.get_i32("n").unwrap(), 0);
    }

    #[test]
    fn find_and_modify_remove_true_deletes_and_returns_doc() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        handle_insert(
            &doc! { "insert": "famremcoll", "documents": [{"tag": "del", "val": 42i32}], "$db": "local" },
            &state,
        );

        let body = doc! {
            "findandmodify": "famremcoll",
            "query": {"tag": "del"},
            "remove": true,
            "$db": "local",
        };
        let result = handle_find_and_modify(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let value = result.get_document("value").unwrap();
        assert_eq!(value.get_i32("val").unwrap(), 42);

        // Verify the document is gone.
        let find_res = handle_find(
            &doc! { "find": "famremcoll", "filter": {}, "$db": "local" },
            &state,
            &cursors,
        );
        let batch = find_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert!(batch.is_empty());
    }

    #[test]
    fn find_and_modify_collation_returns_bad_value() {
        let state = ServerState::default();
        let body = doc! {
            "findandmodify": "col",
            "query": {},
            "update": {"$set": {"x": 1i32}},
            "collation": {"locale": "en"},
            "$db": "local",
        };
        let result = handle_find_and_modify(&body, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 2);
    }

    // ---- CRUD via full OP_MSG dispatch ----

    /// End-to-end dispatch test: insert then find through the wire framing layer.
    #[test]
    fn dispatch_op_msg_insert_and_find() {
        let state = ServerState::default();
        let cursors = dummy_cursors();

        // Insert
        let insert_req = make_op_msg_request(
            100,
            &doc! { "insert": "disp_coll", "documents": [{"hello": "world"}], "$db": "local" },
        );
        let msg = OpMsg::parse(&insert_req).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 200, msg.header.request_id, &state, 1, &cursors).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert_eq!(body.get_i32("n").unwrap(), 1);

        // Find
        let find_req = make_op_msg_request(
            101,
            &doc! { "find": "disp_coll", "filter": {}, "$db": "local" },
        );
        let msg = OpMsg::parse(&find_req).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 201, msg.header.request_id, &state, 1, &cursors).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        let cursor_doc = body.get_document("cursor").unwrap();
        assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 1);
        assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
        assert!(cursor_doc.get_str("ns").unwrap().contains("disp_coll"));
    }

    /// Verify merge_doc_sequences_into_body works for the pymongo insert path.
    #[test]
    fn merge_doc_sequences_merges_kind1_documents() {
        let body = doc! { "insert": "coll", "$db": "local" };
        let docs = vec![doc! { "a": 1i32 }, doc! { "a": 2i32 }];
        let sections = vec![
            Section::Body(body.clone()),
            Section::DocSequence {
                identifier: "documents".to_owned(),
                documents: docs.clone(),
            },
        ];
        let merged = merge_doc_sequences_into_body(&body, &sections);
        let arr = merged.get_array("documents").unwrap();
        assert_eq!(arr.len(), 2);
    }

    /// `get_i64` must coerce Int32, Int64 and Double.
    #[test]
    fn get_i64_coerces_bson_types() {
        let doc = doc! {
            "int32": 7i32,
            "int64": 100i64,
            "double": 3.0_f64,
        };
        assert_eq!(get_i64(&doc, "int32"), Some(7));
        assert_eq!(get_i64(&doc, "int64"), Some(100));
        assert_eq!(get_i64(&doc, "double"), Some(3));
        assert_eq!(get_i64(&doc, "missing"), None);
    }

    // -----------------------------------------------------------------------
    // getMore
    // -----------------------------------------------------------------------

    #[test]
    fn get_more_paginates_through_cursor() {
        let state = ServerState::default();
        let cursors = dummy_cursors();

        // Insert 5 documents.
        handle_insert(
            &doc! { "insert": "pgcoll", "documents": [
                {"i": 0i32}, {"i": 1i32}, {"i": 2i32}, {"i": 3i32}, {"i": 4i32}
            ], "$db": "local" },
            &state,
        );

        // Find with batchSize=2: get first 2, server-side cursor for the rest.
        let find_res = handle_find(
            &doc! { "find": "pgcoll", "filter": {}, "batchSize": 2i32, "$db": "local" },
            &state,
            &cursors,
        );
        let cursor_doc = find_res.get_document("cursor").unwrap();
        let cursor_id = cursor_doc.get_i64("id").unwrap();
        assert_ne!(cursor_id, 0, "first batch should leave a live cursor");
        assert_eq!(cursor_doc.get_array("firstBatch").unwrap().len(), 2);

        // getMore: next 2.
        let more_res = handle_get_more(
            &doc! { "getMore": bson::Bson::Int64(cursor_id), "collection": "pgcoll", "batchSize": 2i32, "$db": "local" },
            &state,
            &cursors,
        );
        assert_eq!(more_res.get_f64("ok").unwrap(), 1.0, "{more_res:?}");
        let more_cursor = more_res.get_document("cursor").unwrap();
        assert_eq!(more_cursor.get_array("nextBatch").unwrap().len(), 2);
        let mid_id = more_cursor.get_i64("id").unwrap();
        assert_ne!(mid_id, 0, "one doc still remains");

        // getMore: last 1.
        let last_res = handle_get_more(
            &doc! { "getMore": bson::Bson::Int64(mid_id), "collection": "pgcoll", "$db": "local" },
            &state,
            &cursors,
        );
        assert_eq!(last_res.get_f64("ok").unwrap(), 1.0, "{last_res:?}");
        let last_cursor = last_res.get_document("cursor").unwrap();
        assert_eq!(last_cursor.get_array("nextBatch").unwrap().len(), 1);
        // Cursor exhausted: id must be 0.
        assert_eq!(
            last_cursor.get_i64("id").unwrap(),
            0,
            "cursor must be exhausted"
        );
        // Cursor removed from map.
        assert_eq!(cursors.lock().unwrap().len(), 0);
    }

    #[test]
    fn get_more_unknown_cursor_returns_cursor_not_found() {
        let state = ServerState::default();
        let cursors = dummy_cursors();
        let result = handle_get_more(
            &doc! { "getMore": bson::Bson::Int64(9999i64), "collection": "c", "$db": "local" },
            &state,
            &cursors,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 0.0);
        assert_eq!(result.get_i32("code").unwrap(), 43); // CursorNotFound
        assert_eq!(result.get_str("codeName").unwrap(), "CursorNotFound");
    }

    // -----------------------------------------------------------------------
    // killCursors
    // -----------------------------------------------------------------------

    #[test]
    fn kill_cursors_removes_known_cursors() {
        let cursors = dummy_cursors();
        // Store two cursors.
        let id1 = cursors
            .lock()
            .unwrap()
            .store(crate::Cursor::<Document>::empty());
        let id2 = cursors
            .lock()
            .unwrap()
            .store(crate::Cursor::<Document>::empty());
        assert_eq!(cursors.lock().unwrap().len(), 2);

        let result = handle_kill_cursors(
            &doc! { "killCursors": "c", "cursors": [bson::Bson::Int64(id1), bson::Bson::Int64(id2)], "$db": "local" },
            &cursors,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let killed = result.get_array("cursorsKilled").unwrap();
        assert_eq!(killed.len(), 2);
        let not_found = result.get_array("cursorsNotFound").unwrap();
        assert!(not_found.is_empty());
        assert_eq!(cursors.lock().unwrap().len(), 0);
    }

    #[test]
    fn kill_cursors_reports_not_found_for_missing_ids() {
        let cursors = dummy_cursors();
        let result = handle_kill_cursors(
            &doc! { "killCursors": "c", "cursors": [bson::Bson::Int64(42i64)], "$db": "local" },
            &cursors,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        assert!(result.get_array("cursorsKilled").unwrap().is_empty());
        assert_eq!(result.get_array("cursorsNotFound").unwrap().len(), 1);
    }

    // -----------------------------------------------------------------------
    // create / drop
    // -----------------------------------------------------------------------

    #[test]
    fn create_collection_returns_ok() {
        let state = ServerState::default();
        let result = handle_create(&doc! { "create": "newcoll", "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn create_collection_is_idempotent() {
        let state = ServerState::default();
        handle_create(&doc! { "create": "idmcoll", "$db": "local" }, &state);
        // Creating again must still return ok:1.
        let result = handle_create(&doc! { "create": "idmcoll", "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn drop_collection_returns_ok() {
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "dropcoll", "documents": [{"x": 1i32}], "$db": "local" },
            &state,
        );
        let result = handle_drop(&doc! { "drop": "dropcoll", "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn drop_nonexistent_collection_returns_ok() {
        let state = ServerState::default();
        let result = handle_drop(&doc! { "drop": "ghost", "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
    }

    // -----------------------------------------------------------------------
    // listCollections
    // -----------------------------------------------------------------------

    #[test]
    fn list_collections_empty_db() {
        let state = ServerState::default();
        let result =
            handle_list_collections(&doc! { "listCollections": 1, "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let cursor_doc = result.get_document("cursor").unwrap();
        assert_eq!(cursor_doc.get_i64("id").unwrap(), 0);
        assert!(cursor_doc.get_array("firstBatch").unwrap().is_empty());
    }

    #[test]
    fn list_collections_after_insert() {
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "alpha", "documents": [{"x": 1i32}], "$db": "local" },
            &state,
        );
        handle_insert(
            &doc! { "insert": "beta", "documents": [{"y": 2i32}], "$db": "local" },
            &state,
        );
        let result =
            handle_list_collections(&doc! { "listCollections": 1, "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let batch = result
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 2);
        // Each entry must have name, type, options, idIndex.
        for entry in batch {
            let doc = entry.as_document().unwrap();
            assert!(doc.contains_key("name"));
            assert_eq!(doc.get_str("type").unwrap(), "collection");
            assert!(doc.contains_key("options"));
            assert!(doc.contains_key("idIndex"));
        }
    }

    #[test]
    fn list_collections_name_filter() {
        let state = ServerState::default();
        handle_insert(
            &doc! { "insert": "matchme", "documents": [{"a": 1i32}], "$db": "local" },
            &state,
        );
        handle_insert(
            &doc! { "insert": "other", "documents": [{"a": 2i32}], "$db": "local" },
            &state,
        );
        let result = handle_list_collections(
            &doc! { "listCollections": 1, "filter": {"name": "matchme"}, "$db": "local" },
            &state,
        );
        let batch = result
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].as_document().unwrap().get_str("name").unwrap(),
            "matchme"
        );
    }

    // -----------------------------------------------------------------------
    // createIndexes / dropIndexes / listIndexes
    // -----------------------------------------------------------------------

    #[test]
    fn create_indexes_returns_num_before_after() {
        let state = ServerState::default();
        let result = handle_create_indexes(
            &doc! {
                "createIndexes": "idxcoll",
                "indexes": [{
                    "key": {"email": 1i32},
                    "name": "email_1",
                }],
                "$db": "local",
            },
            &state,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");
        // Before: only synthetic _id_ (= 1). After: _id_ + email_1 (= 2).
        assert_eq!(result.get_i32("numIndexesBefore").unwrap(), 1);
        assert_eq!(result.get_i32("numIndexesAfter").unwrap(), 2);
    }

    #[test]
    fn create_indexes_unique_flag() {
        let state = ServerState::default();
        handle_create_indexes(
            &doc! {
                "createIndexes": "uniqcoll",
                "indexes": [{"key": {"uid": 1i32}, "name": "uid_1", "unique": true}],
                "$db": "local",
            },
            &state,
        );
        let list_res =
            handle_list_indexes(&doc! { "listIndexes": "uniqcoll", "$db": "local" }, &state);
        let batch = list_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        // _id_ at index 0, uid_1 at index 1.
        let uid_doc = batch[1].as_document().unwrap();
        assert_eq!(uid_doc.get_str("name").unwrap(), "uid_1");
        assert!(uid_doc.get_bool("unique").unwrap());
    }

    #[test]
    fn list_indexes_always_includes_id_index() {
        let state = ServerState::default();
        // Collection with no user-created indexes.
        handle_create(&doc! { "create": "barelidx", "$db": "local" }, &state);
        let result =
            handle_list_indexes(&doc! { "listIndexes": "barelidx", "$db": "local" }, &state);
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let batch = result
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1, "only _id_ index expected");
        let id_idx = batch[0].as_document().unwrap();
        assert_eq!(id_idx.get_str("name").unwrap(), "_id_");
        assert_eq!(id_idx.get_i32("v").unwrap(), 2);
        let key = id_idx.get_document("key").unwrap();
        assert_eq!(key.get_i32("_id").unwrap(), 1);
    }

    #[test]
    fn drop_indexes_by_name() {
        let state = ServerState::default();
        handle_create_indexes(
            &doc! {
                "createIndexes": "dropbynamecoll",
                "indexes": [{"key": {"score": 1i32}, "name": "score_1"}],
                "$db": "local",
            },
            &state,
        );
        let result = handle_drop_indexes(
            &doc! { "dropIndexes": "dropbynamecoll", "index": "score_1", "$db": "local" },
            &state,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        // Verify the index is gone.
        let list_res = handle_list_indexes(
            &doc! { "listIndexes": "dropbynamecoll", "$db": "local" },
            &state,
        );
        let batch = list_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1, "only _id_ should remain");
    }

    #[test]
    fn drop_indexes_star_drops_all_user_indexes() {
        let state = ServerState::default();
        handle_create_indexes(
            &doc! {
                "createIndexes": "staridxcoll",
                "indexes": [
                    {"key": {"a": 1i32}, "name": "a_1"},
                    {"key": {"b": 1i32}, "name": "b_1"},
                ],
                "$db": "local",
            },
            &state,
        );
        let result = handle_drop_indexes(
            &doc! { "dropIndexes": "staridxcoll", "index": "*", "$db": "local" },
            &state,
        );
        assert_eq!(result.get_f64("ok").unwrap(), 1.0);
        let list_res = handle_list_indexes(
            &doc! { "listIndexes": "staridxcoll", "$db": "local" },
            &state,
        );
        let batch = list_res
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1, "only _id_ should remain after drop *");
    }

    // -----------------------------------------------------------------------
    // Full OP_MSG dispatch tests for new commands
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_op_msg_create_and_list_collections() {
        let state = ServerState::default();
        let cursors = dummy_cursors();

        // Create collection via wire protocol.
        let req = make_op_msg_request(200, &doc! { "create": "wiredcoll", "$db": "local" });
        let msg = OpMsg::parse(&req).unwrap();
        let resp_bytes =
            dispatch_op_msg(&msg, 300, msg.header.request_id, &state, 1, &cursors).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        assert_eq!(resp.body().unwrap().get_f64("ok").unwrap(), 1.0);

        // listCollections should show it.
        let req2 = make_op_msg_request(201, &doc! { "listCollections": 1i32, "$db": "local" });
        let msg2 = OpMsg::parse(&req2).unwrap();
        let resp2_bytes =
            dispatch_op_msg(&msg2, 301, msg2.header.request_id, &state, 1, &cursors).unwrap();
        let resp2 = OpMsg::parse(&resp2_bytes).unwrap();
        let body2 = resp2.body().unwrap();
        assert_eq!(body2.get_f64("ok").unwrap(), 1.0);
        let batch = body2
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].as_document().unwrap().get_str("name").unwrap(),
            "wiredcoll"
        );
    }

    #[test]
    fn dispatch_op_msg_get_more_pagination() {
        let state = ServerState::default();
        let cursors = dummy_cursors();

        // Insert 3 docs.
        let ins_req = make_op_msg_request(
            210,
            &doc! { "insert": "gm_coll", "documents": [{"i": 1i32}, {"i": 2i32}, {"i": 3i32}], "$db": "local" },
        );
        let ins_msg = OpMsg::parse(&ins_req).unwrap();
        dispatch_op_msg(
            &ins_msg,
            310,
            ins_msg.header.request_id,
            &state,
            1,
            &cursors,
        )
        .unwrap();

        // Find with batchSize=1.
        let find_req = make_op_msg_request(
            211,
            &doc! { "find": "gm_coll", "filter": {}, "batchSize": 1i32, "$db": "local" },
        );
        let find_msg = OpMsg::parse(&find_req).unwrap();
        let find_resp_bytes = dispatch_op_msg(
            &find_msg,
            311,
            find_msg.header.request_id,
            &state,
            1,
            &cursors,
        )
        .unwrap();
        let find_resp = OpMsg::parse(&find_resp_bytes).unwrap();
        let find_body = find_resp.body().unwrap();
        assert_eq!(find_body.get_f64("ok").unwrap(), 1.0);
        let cursor_id = find_body
            .get_document("cursor")
            .unwrap()
            .get_i64("id")
            .unwrap();
        assert_ne!(cursor_id, 0);

        // getMore.
        let gm_req = make_op_msg_request(
            212,
            &doc! { "getMore": bson::Bson::Int64(cursor_id), "collection": "gm_coll", "batchSize": 10i32, "$db": "local" },
        );
        let gm_msg = OpMsg::parse(&gm_req).unwrap();
        let gm_resp_bytes =
            dispatch_op_msg(&gm_msg, 312, gm_msg.header.request_id, &state, 1, &cursors).unwrap();
        let gm_resp = OpMsg::parse(&gm_resp_bytes).unwrap();
        let gm_body = gm_resp.body().unwrap();
        assert_eq!(gm_body.get_f64("ok").unwrap(), 1.0);
        let gm_cursor = gm_body.get_document("cursor").unwrap();
        // nextBatch must exist (not firstBatch).
        assert!(
            gm_cursor.contains_key("nextBatch"),
            "getMore response must use 'nextBatch'"
        );
        assert!(
            !gm_cursor.contains_key("firstBatch"),
            "getMore must NOT use 'firstBatch'"
        );
        // Remaining 2 docs plus cursor exhausted.
        assert_eq!(gm_cursor.get_array("nextBatch").unwrap().len(), 2);
        assert_eq!(
            gm_cursor.get_i64("id").unwrap(),
            0,
            "cursor must be exhausted"
        );
    }

    #[test]
    fn dispatch_op_msg_create_and_list_indexes() {
        let state = ServerState::default();
        let cursors = dummy_cursors();

        // createIndexes.
        let ci_req = make_op_msg_request(
            220,
            &doc! {
                "createIndexes": "idx_test_coll",
                "indexes": [{"key": {"name": 1i32}, "name": "name_1"}],
                "$db": "local",
            },
        );
        let ci_msg = OpMsg::parse(&ci_req).unwrap();
        let ci_resp_bytes =
            dispatch_op_msg(&ci_msg, 320, ci_msg.header.request_id, &state, 1, &cursors).unwrap();
        let ci_resp = OpMsg::parse(&ci_resp_bytes).unwrap();
        let ci_body = ci_resp.body().unwrap();
        assert_eq!(ci_body.get_f64("ok").unwrap(), 1.0);
        assert_eq!(ci_body.get_i32("numIndexesBefore").unwrap(), 1);
        assert_eq!(ci_body.get_i32("numIndexesAfter").unwrap(), 2);

        // listIndexes.
        let li_req = make_op_msg_request(
            221,
            &doc! { "listIndexes": "idx_test_coll", "$db": "local" },
        );
        let li_msg = OpMsg::parse(&li_req).unwrap();
        let li_resp_bytes =
            dispatch_op_msg(&li_msg, 321, li_msg.header.request_id, &state, 1, &cursors).unwrap();
        let li_resp = OpMsg::parse(&li_resp_bytes).unwrap();
        let li_body = li_resp.body().unwrap();
        assert_eq!(li_body.get_f64("ok").unwrap(), 1.0);
        let batch = li_body
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        // _id_ + name_1
        assert_eq!(batch.len(), 2);
        assert_eq!(
            batch[0].as_document().unwrap().get_str("name").unwrap(),
            "_id_"
        );
        assert_eq!(
            batch[1].as_document().unwrap().get_str("name").unwrap(),
            "name_1"
        );
    }
}
