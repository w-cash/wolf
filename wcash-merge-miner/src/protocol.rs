//! Bounded loopback JSON-lines protocol for local solver interoperability.

use std::{
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use serde_json::{json, Map, Value};

use crate::{MinerError, PreparedJob, EQUIHASH_SOLUTION_BYTES};

/// Maximum accepted JSON request size for the local protocol.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// Maximum number of loopback clients served at the same time.
pub const MAX_CONCURRENT_CLIENTS: usize = 8;

const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(5);

/// Serves one fixed job over a deliberately small Stratum-like interface.
///
/// Only loopback addresses are accepted. Requests and responses are one JSON
/// object per line. At most [`MAX_CONCURRENT_CLIENTS`] are served concurrently;
/// excess connections are closed immediately. A malformed, idle, or reset
/// connection is isolated to its worker and cannot stop the listener. The server
/// is intentionally single-process local tooling; it has no TLS, account system,
/// vardiff, persistence, or parent block submitter.
pub fn serve_loopback(bind: SocketAddr, job: PreparedJob) -> Result<(), MinerError> {
    if !bind.ip().is_loopback() {
        return Err(MinerError::InvalidRequest(
            "the development server only binds loopback addresses".to_string(),
        ));
    }

    let listener = TcpListener::bind(bind)?;
    serve_listener(
        listener,
        Arc::new(job),
        ConnectionLimiter::new(MAX_CONCURRENT_CLIENTS),
        CLIENT_IO_TIMEOUT,
        Arc::new(AtomicBool::new(false)),
    )
}

fn serve_listener(
    listener: TcpListener,
    job: Arc<PreparedJob>,
    limiter: ConnectionLimiter,
    client_io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) -> Result<(), MinerError> {
    while !shutdown.load(Ordering::Acquire) {
        let stream = match listener.accept() {
            Ok((stream, _peer_address)) => stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_RETRY_DELAY);
                continue;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        // Test listeners use nonblocking accept so they can shut down cleanly,
        // and some platforms propagate that mode to accepted sockets. Client
        // workers always use bounded blocking I/O with explicit timeouts.
        if stream.set_nonblocking(false).is_err() {
            continue;
        }

        let Some(permit) = limiter.try_acquire() else {
            // Refuse overload in the accept loop without creating an unbounded
            // queue or thread. Dropping the stream closes this excess client.
            drop(stream);
            continue;
        };
        let job = Arc::clone(&job);

        thread::Builder::new()
            .name("wcash-pool-client".to_string())
            .spawn(move || {
                let _permit = permit;
                // Every client error is deliberately scoped to this worker. A
                // timeout, reset, oversized frame, malformed write, or clean EOF
                // releases the permit without affecting the listener.
                let _connection_result = serve_connection(stream, &job, client_io_timeout);
            })?;
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct ConnectionLimiter {
    active: Arc<AtomicUsize>,
    maximum: usize,
}

impl ConnectionLimiter {
    fn new(maximum: usize) -> Self {
        assert!(maximum > 0, "the connection limit must be positive");
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            maximum,
        }
    }

    fn try_acquire(&self) -> Option<ConnectionPermit> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.maximum).then_some(active + 1)
            })
            .ok()?;

        Some(ConnectionPermit {
            limiter: self.clone(),
        })
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct ConnectionPermit {
    limiter: ConnectionLimiter,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let previous = self.limiter.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a connection permit must be active");
    }
}

fn serve_connection(
    mut stream: TcpStream,
    job: &PreparedJob,
    io_timeout: Duration,
) -> Result<(), MinerError> {
    stream.set_read_timeout(Some(io_timeout))?;
    stream.set_write_timeout(Some(io_timeout))?;
    let read_stream = stream.try_clone()?;
    let mut reader = BufReader::new(read_stream);

    while let Some(frame) = read_frame(&mut reader)? {
        let response = match serde_json::from_slice::<Value>(&frame) {
            Ok(request) => dispatch(job, request),
            Err(error) => rpc_error(Value::Null, -32700, format!("parse error: {error}")),
        };
        serde_json::to_writer(&mut stream, &response)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
    }
    Ok(())
}

fn read_frame<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, MinerError> {
    let mut frame = Vec::new();
    loop {
        let (chunk, consumed, finished) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if frame.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(frame));
            }
            match available.iter().position(|byte| *byte == b'\n') {
                Some(position) => (
                    available[..position].to_vec(),
                    position.saturating_add(1),
                    true,
                ),
                None => (available.to_vec(), available.len(), false),
            }
        };

        if frame.len().saturating_add(chunk.len()) > MAX_REQUEST_BYTES {
            return Err(MinerError::RequestTooLarge(MAX_REQUEST_BYTES));
        }
        frame.extend_from_slice(&chunk);
        reader.consume(consumed);
        if finished {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn dispatch(job: &PreparedJob, request: Value) -> Value {
    let Some(object) = request.as_object() else {
        return rpc_error(Value::Null, -32600, "request must be a JSON object");
    };
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return rpc_error(id, -32600, "request method must be a string");
    };

    match method {
        "mining.subscribe" => rpc_result(
            id,
            json!({
                "protocol": "wcash-zcash-aux/1",
                "job_id": job.job_id(),
            }),
        ),
        "mining.authorize" => rpc_result(id, Value::Bool(true)),
        "mining.get_job" => rpc_result(id, job_json(job)),
        "mining.submit" => match submit(job, object.get("params")) {
            Ok(result) => rpc_result(id, result),
            Err(error) => rpc_error(id, -32001, format!("share rejected: {error}")),
        },
        _ => rpc_error(id, -32601, "method not found"),
    }
}

fn submit(job: &PreparedJob, params: Option<&Value>) -> Result<Value, MinerError> {
    let params = params
        .and_then(Value::as_object)
        .ok_or_else(|| MinerError::InvalidRequest("submit params must be an object".to_string()))?;
    let submitted_job = required_string(params, "job_id")?;
    if submitted_job != job.job_id() {
        return Err(MinerError::InvalidRequest(
            "stale or unknown job_id".to_string(),
        ));
    }
    let nonce = decode_fixed_hex::<32>(required_string(params, "nonce_le")?, "nonce_le")?;
    let solution = decode_fixed_hex::<EQUIHASH_SOLUTION_BYTES>(
        required_string(params, "solution")?,
        "solution",
    )?;
    let solved = job.finalize(&nonce, &solution)?;

    Ok(json!({
        "accepted": true,
        "parent_block_hash_le": hex::encode(solved.parent_block_hash_le()),
        "auxpow_proof": hex::encode(solved.encoded_proof()),
    }))
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, MinerError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| MinerError::InvalidRequest(format!("{field} must be a string")))
}

fn decode_fixed_hex<const N: usize>(
    encoded: &str,
    field: &'static str,
) -> Result<[u8; N], MinerError> {
    let bytes = hex::decode(encoded).map_err(|error| MinerError::InvalidHexField {
        field,
        reason: error.to_string(),
    })?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("decoded to {} bytes, expected {N}", bytes.len()),
        })
}

fn job_json(job: &PreparedJob) -> Value {
    json!({
        "job_id": job.job_id(),
        "algorithm": "Equihash(200,9)",
        "child_block_hash_le": hex::encode(job.child_block_hash()),
        "target_le": hex::encode(job.required_target().to_le_bytes()),
        "parent_header_input": hex::encode(job.parent_header_input()),
        "nonce_bytes": 32,
        "solution_bytes": EQUIHASH_SOLUTION_BYTES,
        "parent_coinbase": hex::encode(job.coinbase_bytes()),
        "parent_coinbase_txid_le": hex::encode(job.coinbase_transaction_id()),
        "parent_transaction_count": 1,
        "auxiliary_nonce": job.auxiliary_nonce(),
    })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: Value, code: i32, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message.into()},
    })
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Read},
        net::{Ipv4Addr, SocketAddrV4},
        thread::JoinHandle,
        time::Instant,
    };

    use wcash_zcash_aux::Target;

    use crate::JobConfig;

    use super::*;

    fn job() -> PreparedJob {
        PreparedJob::new([0x33; 32], Target::MAX, JobConfig::default()).expect("valid fixture job")
    }

    struct TestServer {
        address: SocketAddr,
        shutdown: Arc<AtomicBool>,
        limiter: ConnectionLimiter,
        join: Option<JoinHandle<Result<(), MinerError>>>,
    }

    impl TestServer {
        fn start(maximum_clients: usize, client_io_timeout: Duration) -> Self {
            let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
                .expect("an ephemeral loopback listener binds");
            listener
                .set_nonblocking(true)
                .expect("the test listener becomes nonblocking");
            let address = listener
                .local_addr()
                .expect("bound listener has an address");
            let shutdown = Arc::new(AtomicBool::new(false));
            let limiter = ConnectionLimiter::new(maximum_clients);

            let server_shutdown = Arc::clone(&shutdown);
            let server_limiter = limiter.clone();
            let join = thread::spawn(move || {
                serve_listener(
                    listener,
                    Arc::new(job()),
                    server_limiter,
                    client_io_timeout,
                    server_shutdown,
                )
            });

            Self {
                address,
                shutdown,
                limiter,
                join: Some(join),
            }
        }

        fn connect(&self) -> TcpStream {
            TcpStream::connect(self.address).expect("test client connects")
        }

        fn wait_for_active_clients(&self, expected: usize) {
            let deadline = Instant::now() + Duration::from_secs(3);
            while self.limiter.active() != expected && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                self.limiter.active(),
                expected,
                "server did not reach the expected active-client count"
            );
        }

        fn stop(mut self) {
            self.shutdown.store(true, Ordering::Release);
            let result = self
                .join
                .take()
                .expect("test server has a listener thread")
                .join()
                .expect("test listener thread does not panic");
            result.expect("test listener exits cleanly");
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Release);
            if let Some(join) = self.join.take() {
                let _join_result = join.join();
            }
        }
    }

    fn request(server: &TestServer, id: u64) -> Value {
        let mut stream = server.connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("client read timeout is configured");
        writeln!(
            stream,
            "{}",
            json!({"jsonrpc": "2.0", "id": id, "method": "mining.subscribe"})
        )
        .expect("request is written");
        stream.flush().expect("request is flushed");

        let mut response = String::new();
        BufReader::new(&mut stream)
            .read_line(&mut response)
            .expect("server returns a response");
        serde_json::from_str(&response).expect("server response is JSON")
    }

    #[test]
    fn job_protocol_is_explicit_about_byte_order_and_sizes() {
        let job = job();
        let response = dispatch(
            &job,
            json!({"jsonrpc": "2.0", "id": 7, "method": "mining.get_job"}),
        );
        let result = &response["result"];
        assert_eq!(result["job_id"], job.job_id());
        assert_eq!(result["nonce_bytes"], 32);
        assert_eq!(result["solution_bytes"], EQUIHASH_SOLUTION_BYTES);
        assert_eq!(
            result["parent_header_input"].as_str().map(str::len),
            Some(108 * 2)
        );
        assert!(result.get("target_le").is_some());
    }

    #[test]
    fn fake_and_stale_submissions_are_rejected() {
        let job = job();
        let stale = dispatch(
            &job,
            json!({
                "id": 1,
                "method": "mining.submit",
                "params": {
                    "job_id": "stale",
                    "nonce_le": "00".repeat(32),
                    "solution": "00".repeat(EQUIHASH_SOLUTION_BYTES),
                }
            }),
        );
        assert_eq!(stale["error"]["code"], -32001);

        let fake = dispatch(
            &job,
            json!({
                "id": 2,
                "method": "mining.submit",
                "params": {
                    "job_id": job.job_id(),
                    "nonce_le": "00".repeat(32),
                    "solution": "00".repeat(EQUIHASH_SOLUTION_BYTES),
                }
            }),
        );
        assert_eq!(fake["error"]["code"], -32001);
        assert!(fake["error"]["message"]
            .as_str()
            .expect("error message is a string")
            .contains("invalid Zcash Equihash"));
    }

    #[test]
    fn bounded_reader_accepts_lf_and_crlf_and_rejects_large_frames() {
        let mut input = Cursor::new(b"one\ntwo\r\n".to_vec());
        assert_eq!(
            read_frame(&mut input).expect("first frame"),
            Some(b"one".to_vec())
        );
        assert_eq!(
            read_frame(&mut input).expect("second frame"),
            Some(b"two".to_vec())
        );
        assert_eq!(read_frame(&mut input).expect("eof"), None);

        let mut oversized = Cursor::new(vec![b'x'; MAX_REQUEST_BYTES + 1]);
        assert!(matches!(
            read_frame(&mut oversized),
            Err(MinerError::RequestTooLarge(MAX_REQUEST_BYTES))
        ));
    }

    #[test]
    fn idle_abrupt_and_oversized_clients_do_not_kill_listener() {
        let server = TestServer::start(2, Duration::from_millis(500));

        let idle = server.connect();
        server.wait_for_active_clients(1);
        server.wait_for_active_clients(0);
        drop(idle);
        assert_eq!(request(&server, 1)["id"], 1);
        server.wait_for_active_clients(0);

        let mut abrupt = server.connect();
        abrupt
            .write_all(b"{\"id\":2,\"method\":\"mining.subscribe\"}\n")
            .expect("abrupt client writes a request");
        server.wait_for_active_clients(1);
        drop(abrupt);
        server.wait_for_active_clients(0);
        assert_eq!(request(&server, 2)["id"], 2);
        server.wait_for_active_clients(0);

        let mut oversized = server.connect();
        server.wait_for_active_clients(1);
        let _write_result = oversized.write_all(&vec![b'x'; MAX_REQUEST_BYTES + 1]);
        server.wait_for_active_clients(0);
        drop(oversized);
        assert_eq!(request(&server, 3)["id"], 3);
        server.wait_for_active_clients(0);

        server.stop();
    }

    #[test]
    fn concurrent_client_limit_is_strict_and_recovers_capacity() {
        const TEST_LIMIT: usize = 2;

        let server = TestServer::start(TEST_LIMIT, Duration::from_secs(10));
        let mut first = server.connect();
        let mut second = server.connect();
        // Partial frames keep both workers occupied without asking them to do
        // expensive share validation.
        first.write_all(b"{").expect("first client writes");
        second.write_all(b"{").expect("second client writes");
        server.wait_for_active_clients(TEST_LIMIT);

        let mut excess = server.connect();
        excess
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("excess client read timeout is configured");
        let write_succeeded = excess
            .write_all(b"{\"id\":9,\"method\":\"mining.subscribe\"}\n")
            .is_ok();
        let rejected = if write_succeeded {
            let mut byte = [0; 1];
            match excess.read(&mut byte) {
                Ok(0) => true,
                Err(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                ),
                Ok(_) => false,
            }
        } else {
            true
        };
        assert!(rejected, "a client above the strict limit must be closed");
        assert_eq!(server.limiter.active(), TEST_LIMIT);

        drop(first);
        server.wait_for_active_clients(TEST_LIMIT - 1);
        assert_eq!(request(&server, 10)["id"], 10);
        server.wait_for_active_clients(TEST_LIMIT - 1);

        drop(second);
        server.wait_for_active_clients(0);
        server.stop();
    }
}
