//! TCP listener for the MongoDB wire protocol shim.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::{database::Database, error::Result};
use super::protocol::{MsgHeader, OpMsg, MAX_MESSAGE_SIZE};

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
        let (_tx, _rx) = tokio::sync::oneshot::channel::<()>();
        Err(crate::error::Error::Internal(
            "WireProtocol::bind: wire protocol not yet implemented (Phase 1, see hq-6d0)".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Async framing helpers (used by the future connection handler)
// ---------------------------------------------------------------------------

/// Read exactly one complete OP_MSG message from `stream`.
///
/// The protocol is self-delimiting: the first 4 bytes of the 16-byte header
/// encode the total message length.  We read the header first, validate the
/// declared length, then read the remaining bytes.
///
/// # Errors
///
/// - `Io` – network error
/// - `InvalidWireMessage` – header too short, opcode not supported, message
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
    stream
        .read_exact(&mut msg_buf[MsgHeader::SIZE..])
        .await?;

    // Step 4: parse and validate.
    OpMsg::parse(&msg_buf)
}

/// Write a pre-serialised OP_MSG response to `stream`.
///
/// The caller is responsible for building the response bytes via
/// [`OpMsg::build_response`].
pub async fn write_message(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).await?;
    Ok(())
}

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

    #[tokio::test]
    async fn read_write_round_trip() {
        let (mut client, mut server) = loopback_pair().await;

        // Build a simple response and send it from the "server" side.
        let body = doc! { "ok": 1, "ismaster": true };
        let bytes = OpMsg::build_response(1, 99, &body).unwrap();
        write_message(&mut server, &bytes).await.unwrap();

        // Read it back on the "client" side.
        let msg = read_message(&mut client).await.unwrap();
        assert_eq!(msg.header.request_id, 1);
        assert_eq!(msg.header.response_to, 99);
        let parsed_body = msg.body().unwrap();
        assert_eq!(parsed_body.get_i32("ok").unwrap(), 1);
    }

    #[tokio::test]
    async fn oversized_message_rejected_on_read() {
        let (mut client, mut server) = loopback_pair().await;

        // Send a header claiming 49 MiB.
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
}
