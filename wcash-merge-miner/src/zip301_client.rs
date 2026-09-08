//! Bounded loopback ZIP-301 reference miner used by integration tests.
//!
//! This client deliberately implements the miner side from wire messages instead
//! of borrowing an in-process [`crate::NativePreparedJob`]. It therefore checks
//! that the exact header advertised by the pool can be solved and submitted
//! through the same interface used by an Equihash ASIC.

use std::{
    fmt,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use equihash::tromp::solve_200_9;
use serde_json::{json, Value};
use wcash_zcash_aux::{AuxPowError, ParentHeader, Target};

use crate::{MinerError, EQUIHASH_SOLUTION_BYTES};

const HEADER_INPUT_BYTES: usize = 108;
const NONCE_BYTES: usize = 32;
const NONCE_1_BYTES: usize = 4;
const SOLUTION_PREFIX: [u8; 3] = [0xfd, 0x40, 0x05];
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for one bounded reference-miner connection.
pub struct Zip301ClientConfig {
    endpoint: SocketAddr,
    worker: String,
    password: String,
    maximum_nonce_runs: u64,
    start_nonce: u64,
}

impl Zip301ClientConfig {
    /// Creates a loopback-only reference-miner configuration.
    pub fn new(
        endpoint: SocketAddr,
        worker: String,
        password: String,
        maximum_nonce_runs: u64,
        start_nonce: u64,
    ) -> Result<Self, MinerError> {
        if !endpoint.ip().is_loopback() {
            return Err(MinerError::InvalidRequest(
                "the reference ZIP-301 miner only connects to a loopback address".to_string(),
            ));
        }
        if worker.is_empty()
            || worker.len() > 128
            || worker.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(MinerError::InvalidRequest(
                "ZIP-301 worker name must be a bounded, non-empty single-line string".to_string(),
            ));
        }
        if !(12..=1_024).contains(&password.len()) {
            return Err(MinerError::InvalidRequest(
                "ZIP-301 password must contain 12..=1024 bytes".to_string(),
            ));
        }
        if maximum_nonce_runs == 0 {
            return Err(MinerError::InvalidRequest(
                "reference-miner max-runs must be positive".to_string(),
            ));
        }
        start_nonce
            .checked_add(maximum_nonce_runs)
            .ok_or(MinerError::NonceRangeOverflow)?;

        Ok(Self {
            endpoint,
            worker,
            password,
            maximum_nonce_runs,
            start_nonce,
        })
    }
}

impl fmt::Debug for Zip301ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Zip301ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("worker", &self.worker)
            .field("password", &"[REDACTED]")
            .field("maximum_nonce_runs", &self.maximum_nonce_runs)
            .field("start_nonce", &self.start_nonce)
            .finish()
    }
}

/// A share accepted over the real ZIP-301 wire path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Zip301AcceptedShare {
    job_id: String,
    parent_block_hash: String,
    nonce: [u8; NONCE_BYTES],
    solution: Box<[u8; EQUIHASH_SOLUTION_BYTES]>,
    share_target: Target,
    attempted_nonce_runs: u64,
}

impl Zip301AcceptedShare {
    /// Returns the exact server-issued job identifier.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Returns the solved parent block hash in conventional display order.
    pub fn parent_block_hash(&self) -> &str {
        &self.parent_block_hash
    }

    /// Returns the complete 32-byte ZIP-301 nonce.
    pub const fn nonce(&self) -> &[u8; NONCE_BYTES] {
        &self.nonce
    }

    /// Returns the compressed Equihash `(200, 9)` solution without CompactSize.
    pub fn solution(&self) -> &[u8; EQUIHASH_SOLUTION_BYTES] {
        &self.solution
    }

    /// Returns the pool target used to validate this share.
    pub const fn share_target(&self) -> Target {
        self.share_target
    }

    /// Returns the number of memory-hard nonce runs consumed by the solver.
    pub const fn attempted_nonce_runs(&self) -> u64 {
        self.attempted_nonce_runs
    }
}

/// Connects, subscribes, authorizes, solves one advertised job, and submits it.
pub fn mine_zip301_once(config: Zip301ClientConfig) -> Result<Zip301AcceptedShare, MinerError> {
    let mut stream = TcpStream::connect_timeout(&config.endpoint, CLIENT_TIMEOUT)?;
    stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
    stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);

    write_request(&mut stream, &subscribe_request(config.endpoint))?;
    let subscription = read_message(&mut reader)?;
    let nonce_1 = parse_subscription(&subscription)?;

    write_request(
        &mut stream,
        &json!({
            "id": 2,
            "method": "mining.authorize",
            "params": [&config.worker, &config.password],
        }),
    )?;
    let authorization = read_message(&mut reader)?;
    require_boolean_success(&authorization, 2, "mining.authorize")?;

    let target_message = read_message(&mut reader)?;
    let share_target = parse_target(&target_message)?;
    let notify_message = read_message(&mut reader)?;
    let work = Zip301Work::parse(&notify_message, nonce_1, share_target)?;

    // Common NiceHash/nheqminer clients probe this optional extension. The
    // backend must reject it explicitly instead of silently changing the
    // server-assigned nonce partition.
    write_request(
        &mut stream,
        &json!({
            "id": 3,
            "method": "mining.extranonce.subscribe",
            "params": [],
        }),
    )?;
    let extranonce = read_message(&mut reader)?;
    require_unsupported_extension(&extranonce, 3, "mining.extranonce.subscribe")?;

    let solved = work.solve(config.start_nonce, config.maximum_nonce_runs)?;

    write_request(
        &mut stream,
        &json!({
            "id": 4,
            "method": "mining.submit",
            "params": [
                &config.worker,
                &work.job_id,
                hex::encode(&work.header_input[100..104]),
                hex::encode(&solved.nonce[NONCE_1_BYTES..]),
                encode_solution(&solved.solution),
            ],
        }),
    )?;
    let submission = read_message(&mut reader)?;
    require_boolean_success(&submission, 4, "mining.submit")?;

    Ok(Zip301AcceptedShare {
        job_id: work.job_id,
        parent_block_hash: solved.parent_block_hash,
        nonce: solved.nonce,
        solution: solved.solution,
        share_target,
        attempted_nonce_runs: solved.attempted_nonce_runs,
    })
}

struct Zip301Work {
    job_id: String,
    header_input: [u8; HEADER_INPUT_BYTES],
    nonce_1: [u8; NONCE_1_BYTES],
    share_target: Target,
}

impl Zip301Work {
    fn parse(
        message: &Value,
        nonce_1: [u8; NONCE_1_BYTES],
        share_target: Target,
    ) -> Result<Self, MinerError> {
        require_notification(message, "mining.notify")?;
        let params = message["params"].as_array().ok_or_else(|| {
            MinerError::InvalidRequest("mining.notify params must be an array".to_string())
        })?;
        if params.len() != 8 {
            return Err(MinerError::InvalidRequest(
                "mining.notify must contain eight parameters".to_string(),
            ));
        }
        let job_id = params[0].as_str().ok_or_else(|| {
            MinerError::InvalidRequest("mining.notify job id must be a string".to_string())
        })?;
        if job_id.len() != 64 || hex::decode(job_id).is_err() {
            return Err(MinerError::InvalidRequest(
                "mining.notify job id must contain 32 hexadecimal bytes".to_string(),
            ));
        }
        if params[7].as_bool() != Some(true) {
            return Err(MinerError::InvalidRequest(
                "the one-shot reference miner requires clean_jobs=true".to_string(),
            ));
        }

        let mut header_input = [0; HEADER_INPUT_BYTES];
        let fields = [
            (1, 0, 4, "version"),
            (2, 4, 36, "previous block hash"),
            (3, 36, 68, "transaction Merkle root"),
            (4, 68, 100, "block commitments hash"),
            (5, 100, 104, "time"),
            (6, 104, 108, "bits"),
        ];
        for (parameter, start, end, field) in fields {
            let value = params[parameter].as_str().ok_or_else(|| {
                MinerError::InvalidRequest(format!("mining.notify {field} must be hexadecimal"))
            })?;
            let decoded = decode_exact_hex(value, end - start, field)?;
            header_input[start..end].copy_from_slice(&decoded);
        }

        Ok(Self {
            job_id: job_id.to_ascii_lowercase(),
            header_input,
            nonce_1,
            share_target,
        })
    }

    fn solve(&self, start_nonce: u64, maximum_runs: u64) -> Result<SolvedWireShare, MinerError> {
        let end_nonce = start_nonce
            .checked_add(maximum_runs)
            .ok_or(MinerError::NonceRangeOverflow)?;
        let mut nonce_values = start_nonce..end_nonce;
        let mut attempted = 0u64;

        while attempted < maximum_runs {
            let mut solution_nonce = None;
            let solutions = solve_200_9(&self.header_input, || {
                let value = nonce_values.next()?;
                let nonce = session_nonce(self.nonce_1, value);
                solution_nonce = Some(nonce);
                attempted = attempted.saturating_add(1);
                Some(nonce)
            });
            if solutions.is_empty() {
                break;
            }
            let nonce = solution_nonce.expect("solutions require a generated nonce");
            for solution in solutions {
                let solution: [u8; EQUIHASH_SOLUTION_BYTES] =
                    solution.try_into().map_err(|solution: Vec<u8>| {
                        MinerError::InvalidSolutionLength(solution.len())
                    })?;
                let parent_block_hash = match validate_solved_header(
                    &self.header_input,
                    &nonce,
                    &solution,
                    self.share_target,
                ) {
                    Ok(parent_block_hash) => parent_block_hash,
                    Err(MinerError::AuxPow(AuxPowError::InsufficientParentWork { .. })) => {
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                return Ok(SolvedWireShare {
                    parent_block_hash,
                    nonce,
                    solution: Box::new(solution),
                    attempted_nonce_runs: attempted,
                });
            }
        }

        Err(MinerError::SolverExhausted { attempted })
    }
}

struct SolvedWireShare {
    parent_block_hash: String,
    nonce: [u8; NONCE_BYTES],
    solution: Box<[u8; EQUIHASH_SOLUTION_BYTES]>,
    attempted_nonce_runs: u64,
}

fn session_nonce(nonce_1: [u8; NONCE_1_BYTES], counter: u64) -> [u8; NONCE_BYTES] {
    let mut nonce = [0; NONCE_BYTES];
    nonce[..NONCE_1_BYTES].copy_from_slice(&nonce_1);
    nonce[NONCE_1_BYTES..NONCE_1_BYTES + size_of::<u64>()].copy_from_slice(&counter.to_le_bytes());
    nonce
}

fn validate_solved_header(
    header_input: &[u8; HEADER_INPUT_BYTES],
    nonce: &[u8; NONCE_BYTES],
    solution: &[u8; EQUIHASH_SOLUTION_BYTES],
    target: Target,
) -> Result<String, MinerError> {
    let mut bytes = Vec::with_capacity(
        HEADER_INPUT_BYTES + NONCE_BYTES + SOLUTION_PREFIX.len() + EQUIHASH_SOLUTION_BYTES,
    );
    bytes.extend_from_slice(header_input);
    bytes.extend_from_slice(nonce);
    bytes.extend_from_slice(&SOLUTION_PREFIX);
    bytes.extend_from_slice(solution);
    let header = ParentHeader::decode(&bytes)?;
    let hash = header.validate_work(target)?.block_hash().into_le_bytes();
    Ok(display_hash(hash))
}

fn parse_subscription(message: &Value) -> Result<[u8; NONCE_1_BYTES], MinerError> {
    require_success_envelope(message, 1, "mining.subscribe")?;
    let result = message["result"].as_array().ok_or_else(|| {
        MinerError::InvalidRequest("mining.subscribe result must be an array".to_string())
    })?;
    if result.len() != 2 || !result[0].is_null() {
        return Err(MinerError::InvalidRequest(
            "mining.subscribe result has an unsupported shape".to_string(),
        ));
    }
    let encoded = result[1].as_str().ok_or_else(|| {
        MinerError::InvalidRequest("mining.subscribe nonce_1 must be hexadecimal".to_string())
    })?;
    decode_fixed_hex(encoded, "nonce_1")
}

fn subscribe_request(endpoint: SocketAddr) -> Value {
    json!({
        "id": 1,
        "method": "mining.subscribe",
        "params": [
            "wcash-reference-miner/0.1",
            Value::Null,
            endpoint.ip().to_string(),
            endpoint.port(),
        ],
    })
}

fn parse_target(message: &Value) -> Result<Target, MinerError> {
    require_notification(message, "mining.set_target")?;
    let params = message["params"].as_array().ok_or_else(|| {
        MinerError::InvalidRequest("mining.set_target params must be an array".to_string())
    })?;
    if params.len() != 1 {
        return Err(MinerError::InvalidRequest(
            "mining.set_target must contain one parameter".to_string(),
        ));
    }
    let encoded = params[0].as_str().ok_or_else(|| {
        MinerError::InvalidRequest("mining.set_target target must be hexadecimal".to_string())
    })?;
    let mut big_endian = decode_fixed_hex::<32>(encoded, "share target")?;
    big_endian.reverse();
    Ok(Target::from_le_bytes(big_endian)?)
}

fn require_boolean_success(
    message: &Value,
    expected_id: u64,
    method: &str,
) -> Result<(), MinerError> {
    require_success_envelope(message, expected_id, method)?;
    if message["result"].as_bool() != Some(true) {
        return Err(MinerError::InvalidRequest(format!(
            "{method} was not accepted"
        )));
    }
    Ok(())
}

fn require_unsupported_extension(
    message: &Value,
    expected_id: u64,
    method: &str,
) -> Result<(), MinerError> {
    let object = message.as_object().ok_or_else(|| {
        MinerError::InvalidRequest(format!("{method} response must be an object"))
    })?;
    let error = object.get("error").and_then(Value::as_array);
    if object.get("id").and_then(Value::as_u64) != Some(expected_id)
        || object.get("result").and_then(Value::as_bool) != Some(false)
        || error.is_none_or(|values| {
            values.len() != 3
                || values[0].as_i64() != Some(20)
                || values[1].as_str() != Some("Not supported.")
                || !values[2].is_null()
        })
    {
        return Err(MinerError::InvalidRequest(format!(
            "{method} did not return the required explicit unsupported response"
        )));
    }
    Ok(())
}

fn require_success_envelope(
    message: &Value,
    expected_id: u64,
    method: &str,
) -> Result<(), MinerError> {
    let object = message.as_object().ok_or_else(|| {
        MinerError::InvalidRequest(format!("{method} response must be an object"))
    })?;
    if object.get("id").and_then(Value::as_u64) != Some(expected_id) {
        return Err(MinerError::InvalidRequest(format!(
            "{method} response has an unexpected id"
        )));
    }
    if !object.get("error").is_some_and(Value::is_null) {
        return Err(MinerError::InvalidRequest(format!(
            "{method} returned an error"
        )));
    }
    if !object.contains_key("result") {
        return Err(MinerError::InvalidRequest(format!(
            "{method} response omitted result"
        )));
    }
    Ok(())
}

fn require_notification(message: &Value, method: &str) -> Result<(), MinerError> {
    let object = message.as_object().ok_or_else(|| {
        MinerError::InvalidRequest(format!("{method} notification must be an object"))
    })?;
    if object.get("id").is_none_or(|id| !id.is_null())
        || object.get("method").and_then(Value::as_str) != Some(method)
    {
        return Err(MinerError::InvalidRequest(format!(
            "expected {method} notification"
        )));
    }
    Ok(())
}

fn write_request(stream: &mut TcpStream, request: &Value) -> Result<(), MinerError> {
    serde_json::to_writer(&mut *stream, request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_message(reader: &mut impl BufRead) -> Result<Value, MinerError> {
    let frame = read_bounded_frame(reader)?;
    serde_json::from_slice(&frame).map_err(MinerError::from)
}

fn read_bounded_frame(reader: &mut impl BufRead) -> Result<Vec<u8>, MinerError> {
    let mut frame = Vec::new();
    loop {
        let (chunk, consumed, finished) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Err(MinerError::InvalidRequest(
                    "ZIP-301 server closed before completing a response".to_string(),
                ));
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
        if frame.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(MinerError::RequestTooLarge(MAX_RESPONSE_BYTES));
        }
        frame.extend_from_slice(&chunk);
        reader.consume(consumed);
        if finished {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(frame);
        }
    }
}

fn decode_fixed_hex<const N: usize>(
    encoded: &str,
    field: &'static str,
) -> Result<[u8; N], MinerError> {
    let decoded = decode_exact_hex(encoded, N, field)?;
    decoded
        .try_into()
        .map_err(|bytes: Vec<u8>| MinerError::InvalidHexField {
            field,
            reason: format!("expected {N} bytes, got {}", bytes.len()),
        })
}

fn decode_exact_hex(
    encoded: &str,
    expected_bytes: usize,
    field: &'static str,
) -> Result<Vec<u8>, MinerError> {
    if encoded.len() != expected_bytes.saturating_mul(2) {
        return Err(MinerError::InvalidHexField {
            field,
            reason: format!(
                "expected {} hexadecimal characters, got {}",
                expected_bytes.saturating_mul(2),
                encoded.len()
            ),
        });
    }
    hex::decode(encoded).map_err(|error| MinerError::InvalidHexField {
        field,
        reason: error.to_string(),
    })
}

fn encode_solution(solution: &[u8; EQUIHASH_SOLUTION_BYTES]) -> String {
    let mut encoded = Vec::with_capacity(SOLUTION_PREFIX.len() + solution.len());
    encoded.extend_from_slice(&SOLUTION_PREFIX);
    encoded.extend_from_slice(solution);
    hex::encode(encoded)
}

fn display_hash(mut little_endian: [u8; 32]) -> String {
    little_endian.reverse();
    hex::encode(little_endian)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notify_message(clean_jobs: bool) -> Value {
        json!({
            "id": Value::Null,
            "method": "mining.notify",
            "params": [
                "11".repeat(32),
                "01020304",
                "22".repeat(32),
                "33".repeat(32),
                "44".repeat(32),
                "05060708",
                "090a0b0c",
                clean_jobs,
            ]
        })
    }

    #[test]
    fn parses_canonical_zip301_work_without_shared_job_types() {
        let share_target = Target::MAX;
        let work = Zip301Work::parse(
            &notify_message(true),
            [0xaa, 0xbb, 0xcc, 0xdd],
            share_target,
        )
        .expect("canonical work parses");
        assert_eq!(work.job_id, "11".repeat(32));
        assert_eq!(&work.header_input[..4], &[1, 2, 3, 4]);
        assert_eq!(&work.header_input[4..36], &[0x22; 32]);
        assert_eq!(&work.header_input[36..68], &[0x33; 32]);
        assert_eq!(&work.header_input[68..100], &[0x44; 32]);
        assert_eq!(&work.header_input[100..104], &[5, 6, 7, 8]);
        assert_eq!(&work.header_input[104..108], &[9, 10, 11, 12]);
        assert_eq!(work.share_target, share_target);
    }

    #[test]
    fn subscription_uses_the_canonical_zip301_transcript() {
        let endpoint: SocketAddr = "127.0.0.1:28237".parse().expect("fixture endpoint");
        assert_eq!(
            subscribe_request(endpoint),
            json!({
                "id": 1,
                "method": "mining.subscribe",
                "params": ["wcash-reference-miner/0.1", Value::Null, "127.0.0.1", 28237],
            })
        );
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_wire_configuration() {
        let remote = "192.0.2.1:8237".parse().expect("fixture address");
        assert!(Zip301ClientConfig::new(
            remote,
            "worker.1".to_string(),
            "correct horse battery".to_string(),
            1,
            0,
        )
        .is_err());
        assert!(Zip301Work::parse(&notify_message(false), [0; 4], Target::MAX).is_err());

        let mut malformed = notify_message(true);
        malformed["params"][3] = json!("00");
        assert!(Zip301Work::parse(&malformed, [0; 4], Target::MAX).is_err());
    }

    #[test]
    fn target_and_session_nonce_follow_zip301_byte_order() {
        let target_message = json!({
            "id": Value::Null,
            "method": "mining.set_target",
            "params": [hex::encode((1u8..=32).collect::<Vec<_>>())]
        });
        let target = parse_target(&target_message).expect("target parses");
        assert_eq!(
            target.to_le_bytes(),
            std::array::from_fn(|index| 32 - index as u8)
        );

        let nonce = session_nonce([1, 2, 3, 4], 0x0c0b_0a09_0807_0605);
        assert_eq!(&nonce[..12], &(1u8..=12).collect::<Vec<_>>());
        assert_eq!(&nonce[12..], &[0; 20]);
    }

    #[test]
    fn strict_success_envelopes_do_not_accept_rpc_errors() {
        let success = json!({"id": 2, "result": true, "error": Value::Null});
        require_boolean_success(&success, 2, "mining.authorize").expect("success accepted");
        let error = json!({"id": 2, "result": true, "error": [20, "failure", Value::Null]});
        assert!(require_boolean_success(&error, 2, "mining.authorize").is_err());
        assert!(require_boolean_success(&success, 3, "mining.submit").is_err());

        let unsupported =
            json!({"id": 3, "result": false, "error": [20, "Not supported.", Value::Null]});
        require_unsupported_extension(&unsupported, 3, "mining.extranonce.subscribe")
            .expect("explicit unsupported response accepted");
    }

    #[test]
    fn response_frames_are_bounded_before_allocation_growth() {
        let oversized = vec![b'x'; MAX_RESPONSE_BYTES + 1];
        let mut reader = BufReader::new(std::io::Cursor::new(oversized));
        assert!(matches!(
            read_bounded_frame(&mut reader),
            Err(MinerError::RequestTooLarge(MAX_RESPONSE_BYTES))
        ));
    }
}
