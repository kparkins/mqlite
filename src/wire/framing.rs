// ---------------------------------------------------------------------------
// Async framing helpers (public; used by integration tests and benchmarks)
// ---------------------------------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::protocol::{MsgHeader, OpMsg, MAX_MESSAGE_SIZE};
use crate::error::Result;

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
