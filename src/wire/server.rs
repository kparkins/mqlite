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

use bson::{doc, oid::ObjectId, Document};

use super::protocol::{MsgHeader, OpMsg, Section, MAX_MESSAGE_SIZE};
use crate::{
    client::{Client, ClientInner},
    error::Result,
};

pub(super) use super::framing::{read_message, write_message};
use super::handlers;

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
pub(super) struct ConnectionCursors {
    /// Cursor ID → stored cursor.
    cursors: HashMap<i64, StoredCursor>,
    /// Monotonically increasing cursor ID counter.  Starts at 1; cursor ID 0
    /// is reserved in the MongoDB wire protocol to mean "no cursor".
    next_cursor_id: i64,
}

#[allow(dead_code)] // methods used when data commands (getMore, killCursors) are added
impl ConnectionCursors {
    pub(super) fn new() -> Self {
        ConnectionCursors {
            cursors: HashMap::new(),
            next_cursor_id: 1,
        }
    }

    /// Store `cursor` and return its assigned cursor ID.
    pub(super) fn store(&mut self, cursor: crate::Cursor<bson::Document>) -> i64 {
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
    pub(super) fn remove(&mut self, id: i64) -> Option<crate::Cursor<bson::Document>> {
        self.cursors.remove(&id).map(|e| e.cursor)
    }

    /// Return a mutable reference to the cursor for `id`, refreshing its
    /// last-accessed timestamp.  Returns `None` if the cursor is not found.
    pub(super) fn get_mut(&mut self, id: i64) -> Option<&mut crate::Cursor<bson::Document>> {
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
pub(super) struct ServerState {
    /// Time when this `WireProtocol` instance was started.
    /// Used to compute uptime in the `serverStatus` response.
    pub(super) start_time: Arc<std::time::Instant>,

    /// Monotonically increasing counter used to assign unique per-connection IDs.
    /// Starts at 1; each new connection receives the old value before increment.
    pub(super) next_connection_id: Arc<AtomicI32>,

    /// Path to the database file.
    /// Used to locate the journal file (`<path>-journal`) for `serverStatus`.
    pub(super) db_path: Option<std::path::PathBuf>,

    /// `topologyVersion.processId` — a random [`ObjectId`] generated once at
    /// server start and included in every `hello` / `isMaster` response.
    pub(super) topology_process_id: ObjectId,

    /// Shared client inner state — used by CRUD command handlers.
    pub(super) database: Arc<ClientInner>,

    /// Keeps the temp directory alive for the lifetime of this state.
    /// Only populated when `ServerState` is constructed without an explicit
    /// database path (i.e., in tests via `default()` or `new()`).
    #[cfg(test)]
    pub(super) _tempdir: Option<Arc<tempfile::TempDir>>,
}

#[cfg(test)]
impl Default for ServerState {
    fn default() -> Self {
        let tempdir = tempfile::TempDir::new().expect("create tempdir for default ServerState");
        let db_path = tempdir.path().join("mqlite_test.db");
        let client = Client::open(&db_path).expect("open tempdir-backed client");
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: Some(db_path.clone()),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
            _tempdir: Some(Arc::new(tempdir)),
        }
    }
}

impl ServerState {
    /// Create state backed by a tempdir-scoped [`Client`] for use in tests.
    /// `db_path` is recorded as-is so callers can pass an explicit path or
    /// `None` when the exact path does not matter.
    #[cfg(test)]
    pub(super) fn new(db_path: Option<std::path::PathBuf>) -> Self {
        let tempdir = tempfile::TempDir::new().expect("create tempdir for ServerState::new");
        let tmp_db_path = tempdir.path().join("mqlite_test.db");
        let client = Client::open(&tmp_db_path).expect("open tempdir-backed client");
        ServerState {
            start_time: Arc::new(std::time::Instant::now()),
            next_connection_id: Arc::new(AtomicI32::new(1)),
            db_path: Some(db_path.unwrap_or(tmp_db_path)),
            topology_process_id: ObjectId::new(),
            database: Arc::clone(&client.inner),
            _tempdir: Some(Arc::new(tempdir)),
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
            #[cfg(test)]
            _tempdir: None,
        }
    }

    /// Reserve and return the next connection ID (pre-increment).
    pub(super) fn next_conn_id(&self) -> i32 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Return server uptime in whole seconds.
    pub(super) fn uptime_secs(&self) -> i64 {
        self.start_time.elapsed().as_secs() as i64
    }

    /// Return the size of the journal file in bytes, or 0 if absent.
    pub(super) fn journal_file_size(&self) -> u64 {
        let journal_path = match &self.db_path {
            Some(p) => {
                let mut s = p.as_os_str().to_owned();
                s.push("-journal");
                std::path::PathBuf::from(s)
            }
            None => return 0,
        };
        std::fs::metadata(&journal_path).map(|m| m.len()).unwrap_or(0)
    }

    /// Total number of connections that have been opened since server start.
    pub(super) fn total_connections(&self) -> i32 {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
