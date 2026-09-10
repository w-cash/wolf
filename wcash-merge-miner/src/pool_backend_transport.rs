//! Bounded blocking transport helpers for the private Unix backend socket.
//!
//! Socket deadlines and peer authorization belong to the listener. These
//! helpers enforce the shared frame contract and never allocate a declared
//! payload before checking its hard protocol limit.

use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};

use thiserror::Error;
use wcash_pool_protocol::{
    decode_backend_request, encode_backend_message, BackendMessage, BackendRequest, ProtocolError,
    BACKEND_LENGTH_PREFIX_BYTES, MAX_BACKEND_PAYLOAD_BYTES,
};

/// Framing or I/O failure on a local backend connection.
#[derive(Debug, Error)]
pub enum BackendTransportError {
    /// The peer closed before completing a started frame.
    #[error("backend connection ended during a frame")]
    TruncatedFrame,
    /// The declared payload violates the bounded framing contract.
    #[error("invalid backend frame length")]
    InvalidFrameLength(#[source] ProtocolError),
    /// The complete frame violates the shared protocol schema.
    #[error("invalid backend frame")]
    InvalidFrame(#[source] ProtocolError),
    /// A local socket read or write failed.
    #[error("backend transport I/O failed")]
    Io(#[source] io::Error),
    /// A bounded frame or idle deadline expired.
    #[error("backend transport deadline expired")]
    Timeout,
}

impl BackendTransportError {
    fn io(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::UnexpectedEof => Self::TruncatedFrame,
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => Self::Timeout,
            _ => Self::Io(error),
        }
    }
}

/// Reads one request from a Unix stream with distinct idle and whole-frame deadlines.
///
/// The assembly deadline begins with the first received byte and decreases on
/// every subsequent read, preventing a peer from keeping the connection alive
/// by sending one byte per socket timeout.
pub(crate) fn read_unix_backend_request(
    stream: &mut UnixStream,
    idle_timeout: Duration,
    frame_timeout: Duration,
) -> Result<Option<BackendRequest>, BackendTransportError> {
    if idle_timeout.is_zero() || frame_timeout.is_zero() {
        return Err(BackendTransportError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend transport deadlines must be positive",
        )));
    }
    let mut reader = DeadlineUnixReader {
        stream,
        idle_timeout,
        frame_timeout,
        frame_deadline: None,
    };
    read_backend_request(&mut reader)
}

/// Writes a complete response batch under one absolute deadline.
///
/// Re-applying the full timeout to every frame would let a peer stretch a
/// bounded response indefinitely by reading only a few bytes per interval.
pub(crate) fn write_unix_backend_messages(
    stream: &mut UnixStream,
    messages: &[BackendMessage],
    timeout: Duration,
) -> Result<(), BackendTransportError> {
    if timeout.is_zero() {
        return Err(BackendTransportError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend transport deadlines must be positive",
        )));
    }
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        BackendTransportError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backend write deadline overflowed",
        ))
    })?;
    let mut writer = DeadlineUnixWriter { stream, deadline };
    for message in messages {
        write_backend_message(&mut writer, message)?;
    }
    Ok(())
}

struct DeadlineUnixReader<'a> {
    stream: &'a mut UnixStream,
    idle_timeout: Duration,
    frame_timeout: Duration,
    frame_deadline: Option<Instant>,
}

impl Read for DeadlineUnixReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let now = Instant::now();
        let timeout = match self.frame_deadline {
            Some(deadline) => deadline.checked_duration_since(now).ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "backend frame deadline expired")
            })?,
            None => self.idle_timeout,
        };
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "backend frame deadline expired",
            ));
        }
        self.stream.set_read_timeout(Some(timeout))?;
        let received = self.stream.read(output)?;
        if received > 0 && self.frame_deadline.is_none() {
            self.frame_deadline = Instant::now().checked_add(self.frame_timeout);
            if self.frame_deadline.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "backend frame deadline overflowed",
                ));
            }
        }
        Ok(received)
    }
}

struct DeadlineUnixWriter<'a> {
    stream: &'a mut UnixStream,
    deadline: Instant,
}

impl DeadlineUnixWriter<'_> {
    fn apply_remaining_timeout(&self) -> io::Result<()> {
        let timeout = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|timeout| !timeout.is_zero())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "backend write deadline expired")
            })?;
        self.stream.set_write_timeout(Some(timeout))
    }
}

impl Write for DeadlineUnixWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.apply_remaining_timeout()?;
        self.stream.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.apply_remaining_timeout()?;
        self.stream.flush()
    }
}

/// Reads one exact backend request.
///
/// A clean EOF before any prefix byte returns `Ok(None)`. Once any byte of a
/// frame arrives, EOF is a protocol failure so callers cannot confuse a
/// truncated request with an orderly connection close.
pub(crate) fn read_backend_request(
    reader: &mut impl Read,
) -> Result<Option<BackendRequest>, BackendTransportError> {
    let mut prefix = [0u8; BACKEND_LENGTH_PREFIX_BYTES];
    let first = reader
        .read(&mut prefix[..1])
        .map_err(BackendTransportError::io)?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut prefix[1..])
        .map_err(BackendTransportError::io)?;

    let declared = u32::from_be_bytes(prefix) as usize;
    if declared == 0 {
        let error = decode_backend_request(&prefix)
            .expect_err("a zero-length backend frame is invalid by construction");
        return Err(BackendTransportError::InvalidFrameLength(error));
    }
    if declared > MAX_BACKEND_PAYLOAD_BYTES {
        // The shared decoder produces the canonical typed error without ever
        // seeing or allocating the attacker's declared payload.
        let error = decode_backend_request(&prefix)
            .expect_err("an oversized backend frame is invalid by construction");
        return Err(BackendTransportError::InvalidFrameLength(error));
    }

    let frame_len = BACKEND_LENGTH_PREFIX_BYTES
        .checked_add(declared)
        .expect("the protocol payload bound fits in usize with its four-byte prefix");
    let mut frame = vec![0; frame_len];
    frame[..BACKEND_LENGTH_PREFIX_BYTES].copy_from_slice(&prefix);
    reader
        .read_exact(&mut frame[BACKEND_LENGTH_PREFIX_BYTES..])
        .map_err(BackendTransportError::io)?;
    decode_backend_request(&frame)
        .map(Some)
        .map_err(BackendTransportError::InvalidFrame)
}

/// Encodes, writes, and flushes one validated backend response or event.
pub(crate) fn write_backend_message(
    writer: &mut impl Write,
    message: &BackendMessage,
) -> Result<(), BackendTransportError> {
    let frame = encode_backend_message(message).map_err(BackendTransportError::InvalidFrame)?;
    writer
        .write_all(&frame)
        .map_err(BackendTransportError::io)?;
    writer.flush().map_err(BackendTransportError::io)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor, Read, Write},
        os::unix::net::UnixStream,
        thread,
        time::{Duration, Instant},
    };

    use serde_json::json;
    use wcash_pool_protocol::{
        encode_backend_request, BackendErrorCode, BackendMessage, BackendRequest, CanonicalUuid,
        BACKEND_PROTOCOL_VERSION, MAX_BACKEND_PAYLOAD_BYTES,
    };

    use super::{
        read_backend_request, read_unix_backend_request, write_backend_message,
        write_unix_backend_messages, BackendTransportError, BACKEND_LENGTH_PREFIX_BYTES,
    };

    fn pool_instance() -> CanonicalUuid {
        serde_json::from_value(json!("faab65e2-f272-44cc-b186-c2338750cdb4"))
            .expect("the test UUID uses canonical lowercase syntax")
    }

    fn health(id: u64) -> BackendRequest {
        BackendRequest::Health {
            version: BACKEND_PROTOCOL_VERSION,
            id,
        }
    }

    #[test]
    fn clean_eof_is_distinct_from_a_truncated_frame() {
        assert!(read_backend_request(&mut Cursor::new(Vec::<u8>::new()))
            .unwrap()
            .is_none());
        assert!(matches!(
            read_backend_request(&mut Cursor::new(vec![0, 0, 0])),
            Err(BackendTransportError::TruncatedFrame)
        ));

        let frame = encode_backend_request(&health(1)).unwrap();
        assert!(matches!(
            read_backend_request(&mut Cursor::new(&frame[..frame.len() - 1])),
            Err(BackendTransportError::TruncatedFrame)
        ));
    }

    #[test]
    fn zero_and_oversized_lengths_are_rejected_before_payload_reads() {
        assert!(matches!(
            read_backend_request(&mut Cursor::new(0u32.to_be_bytes())),
            Err(BackendTransportError::InvalidFrameLength(_))
        ));
        let oversized = u32::try_from(MAX_BACKEND_PAYLOAD_BYTES + 1)
            .expect("the bounded test size fits in u32")
            .to_be_bytes();
        assert!(matches!(
            read_backend_request(&mut Cursor::new(oversized)),
            Err(BackendTransportError::InvalidFrameLength(_))
        ));
    }

    #[test]
    fn complete_invalid_json_and_schema_are_rejected() {
        let mut malformed = Vec::from(1u32.to_be_bytes());
        malformed.push(b'{');
        assert!(matches!(
            read_backend_request(&mut Cursor::new(malformed)),
            Err(BackendTransportError::InvalidFrame(_))
        ));

        let payload = br#"{"type":"health","v":1,"id":1,"extra":true}"#;
        let mut unknown = Vec::from(
            u32::try_from(payload.len())
                .expect("the test payload length fits in u32")
                .to_be_bytes(),
        );
        unknown.extend_from_slice(payload);
        assert!(matches!(
            read_backend_request(&mut Cursor::new(unknown)),
            Err(BackendTransportError::InvalidFrame(_))
        ));
    }

    #[test]
    fn coalesced_frames_are_consumed_one_at_a_time() {
        let first = encode_backend_request(&health(11)).unwrap();
        let second = encode_backend_request(&health(12)).unwrap();
        let mut bytes = first;
        bytes.extend_from_slice(&second);
        let mut cursor = Cursor::new(bytes);

        assert_eq!(read_backend_request(&mut cursor).unwrap(), Some(health(11)));
        assert_eq!(read_backend_request(&mut cursor).unwrap(), Some(health(12)));
        assert!(read_backend_request(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn response_write_is_exact_and_flushed() {
        let response = BackendMessage::Error {
            version: BACKEND_PROTOCOL_VERSION,
            id: 9,
            code: BackendErrorCode::Overloaded,
            message: "backend admission queue is full".to_string(),
        };
        let expected = wcash_pool_protocol::encode_backend_message(&response).unwrap();
        let mut writer = RecordingWriter::default();
        write_backend_message(&mut writer, &response).unwrap();
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.flushes, 1);
    }

    #[derive(Default)]
    struct RecordingWriter {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl Write for RecordingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn partial_prefix_reads_are_supported() {
        let frame = encode_backend_request(&BackendRequest::Hello {
            version: BACKEND_PROTOCOL_VERSION,
            id: 1,
            pool_instance: pool_instance(),
            last_event_seq: 0,
        })
        .unwrap();
        let mut reader = OneByteReader::new(frame);
        assert!(matches!(
            read_backend_request(&mut reader).unwrap(),
            Some(BackendRequest::Hello { id: 1, .. })
        ));
    }

    #[test]
    fn unix_idle_deadline_is_bounded() {
        let (mut reader, _writer) = UnixStream::pair().unwrap();
        assert!(matches!(
            read_unix_backend_request(
                &mut reader,
                Duration::from_millis(25),
                Duration::from_secs(1)
            ),
            Err(BackendTransportError::Timeout)
        ));
    }

    #[test]
    fn unix_frame_deadline_is_absolute_after_the_first_byte() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        let frame = encode_backend_request(&health(1)).unwrap();
        let worker = thread::spawn(move || {
            writer.write_all(&frame[..1]).unwrap();
            thread::sleep(Duration::from_millis(100));
            let _ = writer.write_all(&frame[1..]);
        });
        assert!(matches!(
            read_unix_backend_request(
                &mut reader,
                Duration::from_secs(1),
                Duration::from_millis(25)
            ),
            Err(BackendTransportError::Timeout)
        ));
        worker.join().unwrap();
    }

    #[test]
    fn unix_write_batch_has_one_absolute_deadline() {
        let (mut writer, _reader) = UnixStream::pair().unwrap();
        nix::sys::socket::setsockopt(&writer, nix::sys::socket::sockopt::SndBuf, &1_024).unwrap();
        let message = BackendMessage::Error {
            version: BACKEND_PROTOCOL_VERSION,
            id: 1,
            code: BackendErrorCode::Overloaded,
            message: "x".repeat(512),
        };
        let messages = vec![message; 8_192];

        let started = Instant::now();
        assert!(matches!(
            write_unix_backend_messages(&mut writer, &messages, Duration::from_millis(40)),
            Err(BackendTransportError::Timeout)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    struct OneByteReader {
        cursor: Cursor<Vec<u8>>,
    }

    impl OneByteReader {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                cursor: Cursor::new(bytes),
            }
        }
    }

    impl Read for OneByteReader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let maximum = output.len().min(1);
            self.cursor.read(&mut output[..maximum])
        }
    }

    #[test]
    fn prefix_width_matches_the_shared_protocol() {
        assert_eq!(BACKEND_LENGTH_PREFIX_BYTES, 4);
    }
}
