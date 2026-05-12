use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::protocol::{MsgHeader, OpMsg, MAX_MESSAGE_SIZE};
use crate::error::{Error, Result};

/// Default read timeout for `read_message`.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Read exactly one complete OP_MSG message from `stream`.
///
/// Both reads are wrapped in a 60-second timeout; an idle or stalled
/// connection is dropped rather than held open indefinitely.
///
/// # Errors
///
/// - `Io` — network error or read timeout
/// - `InvalidWireMessage` — header too short, opcode not supported, message
///   exceeds `MAX_MESSAGE_SIZE`, or checksum mismatch
pub async fn read_message(stream: &mut TcpStream) -> Result<OpMsg> {
    let mut header_buf = [0u8; MsgHeader::SIZE];
    tokio::time::timeout(READ_TIMEOUT, stream.read_exact(&mut header_buf))
        .await
        .map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "read_message: timed out waiting for message header",
            ))
        })??;

    let declared_len = MsgHeader::parse(&header_buf)?.message_length as usize;

    if declared_len < MsgHeader::SIZE {
        return Err(Error::InvalidWireMessage {
            detail: format!(
                "messageLength {} is smaller than header size {}",
                declared_len,
                MsgHeader::SIZE
            ),
        });
    }
    if declared_len > MAX_MESSAGE_SIZE {
        return Err(Error::InvalidWireMessage {
            detail: format!(
                "message size {} exceeds maximum {} bytes (48 MiB)",
                declared_len, MAX_MESSAGE_SIZE
            ),
        });
    }

    let mut msg_buf = vec![0u8; declared_len];
    msg_buf[..MsgHeader::SIZE].copy_from_slice(&header_buf);

    tokio::time::timeout(
        READ_TIMEOUT,
        stream.read_exact(&mut msg_buf[MsgHeader::SIZE..]),
    )
    .await
    .map_err(|_| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "read_message: timed out waiting for message body",
        ))
    })??;

    OpMsg::parse(&msg_buf)
}

/// Write a pre-serialised OP_MSG response to `stream`.
///
/// # Errors
///
/// Returns an I/O error if the socket write fails.
pub async fn write_message(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).await?;
    Ok(())
}
