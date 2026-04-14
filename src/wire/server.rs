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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use bson::{doc, DateTime, Document};

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
    pub fn bind(_db: &Database, addr: &str) -> Result<WireProtocol> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Channel to report bind success/failure back to the caller synchronously.
        let (bind_tx, bind_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();

        let addr = addr.to_owned();

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
                    _ = accept_loop(listener) => {}
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
async fn accept_loop(listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                tokio::spawn(handle_connection(stream));
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
async fn handle_connection(mut stream: TcpStream) {
    let mut next_request_id: i32 = 1;

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
                match dispatch_op_query(&full, next_request_id, request_id) {
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
                match dispatch_op_msg(&msg, next_request_id, request_id) {
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
// Command dispatch
// ---------------------------------------------------------------------------

/// Dispatch an OP_QUERY message, returning a serialised OP_REPLY response.
fn dispatch_op_query(full_msg: &[u8], request_id: i32, response_to: i32) -> Result<Vec<u8>> {
    // OP_QUERY body starts after the 16-byte header.
    let body_buf = &full_msg[MsgHeader::SIZE..];
    let doc = parse_op_query_body(body_buf)?;
    let command_name = doc
        .keys()
        .next()
        .ok_or_else(|| crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY command document is empty".into(),
        })?;
    let response_body = route_command(command_name);
    build_op_reply(request_id, response_to, &response_body)
}

/// Dispatch an OP_MSG message, returning a serialised OP_MSG response.
fn dispatch_op_msg(msg: &OpMsg, request_id: i32, response_to: i32) -> Result<Vec<u8>> {
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
    let response_body = route_command(command_name);
    OpMsg::build_response(request_id, response_to, &response_body)
}

/// Route a command name to the appropriate handler.
fn route_command(command_name: &str) -> Document {
    match command_name.to_ascii_lowercase().as_str() {
        "hello" | "ismaster" => handle_hello(),
        "ping" => handle_ping(),
        "buildinfo" => handle_build_info(),
        other => handle_unknown(other),
    }
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
/// - No sessions, no auth, no transactions — strips capabilities mqlite lacks
/// - `mqlite.version` so tooling can detect it is talking to mqlite
///
/// See api.md §Handshake Response Design for the full field rationale.
fn handle_hello() -> Document {
    doc! {
        // Standalone — no replica set discovery.
        "isWritablePrimary": true,

        // Signals that the server supports `hello` — pymongo 4.x will use
        // `hello` via OP_MSG for all subsequent topology checks instead of
        // retrying with legacy `isMaster` via OP_QUERY.
        "helloOk": true,

        // Capacity limits (match MongoDB 8.0 defaults).
        "maxBsonObjectSize": 16_777_216i32,
        "maxMessageSizeBytes": 48_000_000i32,
        "maxWriteBatchSize": 100_000i32,

        // Current server time (used by drivers for clock skew detection).
        "localTime": DateTime::now(),

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
fn handle_build_info() -> Document {
    doc! {
        "version": env!("CARGO_PKG_VERSION"),
        "versionArray": [0i32, 1i32, 0i32, 0i32],
        "gitVersion": "mqlite",
        "sysInfo": "Rust",
        // Identify ourselves clearly — do not claim to be MongoDB.
        "engine": "mqlite",
        "ok": 1.0_f64,
    }
}

/// Unknown command — returns `CommandNotFound` (error code 59).
fn handle_unknown(name: &str) -> Document {
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
        let req_buf = make_op_msg_request(1, &doc! { "ping": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 10, msg.header.request_id).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn dispatch_op_msg_hello() {
        let req_buf = make_op_msg_request(2, &doc! { "hello": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 11, msg.header.request_id).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_bool("isWritablePrimary").unwrap());
        assert!(body.get_bool("helloOk").unwrap());
        assert_eq!(body.get_i32("maxWireVersion").unwrap(), 21);
        assert_eq!(body.get_i32("minWireVersion").unwrap(), 0);
    }

    #[test]
    fn dispatch_op_query_ismaster() {
        let req_buf = make_op_query_request(
            3,
            "admin.$cmd",
            &doc! { "ismaster": 1, "helloOk": true },
        );
        let resp_bytes = dispatch_op_query(&req_buf, 12, 3).unwrap();

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
    }

    #[test]
    fn dispatch_op_msg_ismaster() {
        let req_buf = make_op_msg_request(3, &doc! { "ismaster": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 12, msg.header.request_id).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert!(body.get_bool("isWritablePrimary").unwrap());
    }

    #[test]
    fn dispatch_op_msg_build_info() {
        let req_buf = make_op_msg_request(4, &doc! { "buildInfo": 1, "$db": "admin" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 13, msg.header.request_id).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 1.0);
        assert!(body.get_str("version").is_ok());
    }

    #[test]
    fn dispatch_op_msg_unknown_command() {
        let req_buf = make_op_msg_request(5, &doc! { "aggregate": 1, "$db": "test" });
        let msg = OpMsg::parse(&req_buf).unwrap();
        let resp_bytes = dispatch_op_msg(&msg, 14, msg.header.request_id).unwrap();
        let resp = OpMsg::parse(&resp_bytes).unwrap();
        let body = resp.body().unwrap();
        assert_eq!(body.get_f64("ok").unwrap(), 0.0);
        assert_eq!(body.get_i32("code").unwrap(), 59);
        assert_eq!(body.get_str("codeName").unwrap(), "CommandNotFound");
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
    }
}
