//! Bounded, credential-safe JSON-RPC transport for native Zcash nodes.

use std::{
    collections::HashSet,
    fmt,
    io::Read,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::{Client, Response},
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_TYPE},
    redirect::Policy,
    Url,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::{
    rpc_evidence::{PreparationRpcContext, PreparationRpcFailure, RpcMethod, RpcRole},
    MinerError,
};

/// Maximum accepted JSON-RPC response size, including a hex-encoded 2 MB block template.
pub const MAX_RPC_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RPC_RESPONSE_BYTES_U64: u64 = 8 * 1024 * 1024;

/// Default deadline for a single parent-node RPC call.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(15);

/// One authenticated parent-node endpoint.
///
/// Plain HTTP is accepted only for a loopback host. Remote endpoints must use
/// HTTPS so Basic credentials and block submissions are not exposed in transit.
#[derive(Clone)]
pub struct RpcEndpoint {
    url: Url,
    username: Option<String>,
    password: Option<String>,
    label: String,
}

impl RpcEndpoint {
    /// Parses and validates a parent-node endpoint.
    pub fn new(
        url: impl AsRef<str>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self, MinerError> {
        if username.is_some() != password.is_some() {
            return Err(MinerError::RpcConfiguration(
                "RPC username and password must be supplied together".to_string(),
            ));
        }

        let url = Url::parse(url.as_ref())
            .map_err(|error| MinerError::RpcConfiguration(format!("invalid RPC URL: {error}")))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(MinerError::RpcConfiguration(
                "RPC credentials must not be embedded in the URL".to_string(),
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(MinerError::RpcConfiguration(
                "RPC URL must not contain a query or fragment".to_string(),
            ));
        }
        if url.path() != "/" {
            return Err(MinerError::RpcConfiguration(
                "RPC URL must use the root path; path tokens are forbidden".to_string(),
            ));
        }

        match url.scheme() {
            "https" => {}
            "http" if is_loopback_host(&url) => {}
            "http" => {
                return Err(MinerError::RpcConfiguration(
                    "remote parent RPC endpoints must use HTTPS".to_string(),
                ))
            }
            scheme => {
                return Err(MinerError::RpcConfiguration(format!(
                    "unsupported RPC URL scheme {scheme:?}"
                )))
            }
        }

        let host = url
            .host_str()
            .ok_or_else(|| MinerError::RpcConfiguration("RPC URL has no host".to_string()))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| MinerError::RpcConfiguration("RPC URL has no port".to_string()))?;
        let label = format!("{}://{host}:{port}/", url.scheme());

        Ok(Self {
            url,
            username,
            password,
            label,
        })
    }

    /// Returns a credential-free endpoint label for logs and errors.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns true when this endpoint is bound to the local host.
    ///
    /// Native template sources are restricted to loopback because they choose
    /// both coinbase recipients. Independent proposal validators may be remote
    /// over HTTPS, but they cannot prove who receives either block reward.
    pub fn is_loopback(&self) -> bool {
        is_loopback_host(&self.url)
    }
}

impl fmt::Debug for RpcEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcEndpoint")
            .field("url", &self.label)
            .field("authenticated", &self.username.is_some())
            .finish()
    }
}

/// Synchronous bounded JSON-RPC client used by the native template pipeline.
#[derive(Clone, Debug)]
pub struct ZebraRpcClient {
    endpoint: RpcEndpoint,
    client: Client,
    next_id: Arc<AtomicU64>,
    active_coinbase_prewarms: Arc<Mutex<HashSet<u32>>>,
    preparation_context: Option<PreparationAttribution>,
}

#[derive(Clone, Debug)]
struct PreparationAttribution {
    role: RpcRole,
    index: Option<u16>,
    context: PreparationRpcContext,
}

struct CoinbasePrewarmReservation {
    active_heights: Arc<Mutex<HashSet<u32>>>,
    template_height: u32,
}

impl Drop for CoinbasePrewarmReservation {
    fn drop(&mut self) {
        if let Ok(mut active_heights) = self.active_heights.lock() {
            active_heights.remove(&self.template_height);
        }
    }
}

impl ZebraRpcClient {
    /// Creates a hardened client with redirects disabled and a fixed request deadline.
    pub fn new(endpoint: RpcEndpoint, timeout: Duration) -> Result<Self, MinerError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(MinerError::RpcConfiguration(
                "RPC timeout must be between 1 ns and 60 seconds".to_string(),
            ));
        }

        let client = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            // RPC endpoints are explicit operator-pinned security boundaries.
            // In particular, a literal loopback template URL must never be
            // rerouted through an ambient HTTP(S)_PROXY and expose credentials,
            // payout-sensitive templates, or block submissions off-host.
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .map_err(MinerError::RpcTransport)?;

        Ok(Self {
            endpoint,
            client,
            next_id: Arc::new(AtomicU64::new(1)),
            active_coinbase_prewarms: Arc::new(Mutex::new(HashSet::new())),
            preparation_context: None,
        })
    }

    /// Returns the credential-free endpoint label.
    pub fn label(&self) -> &str {
        self.endpoint.label()
    }

    /// Adds only fixed preparation metadata to a temporary shared-client clone.
    pub(crate) fn with_preparation_context(
        &self,
        role: RpcRole,
        index: usize,
        context: PreparationRpcContext,
    ) -> Self {
        let mut client = self.clone();
        let index = u16::try_from(index).ok();
        client.preparation_context = Some(PreparationAttribution {
            role: if index.is_some() {
                role
            } else {
                RpcRole::Unspecified
            },
            index,
            context,
        });
        client
    }

    /// Removes attempt attribution before a client enters long-lived work.
    pub(crate) fn without_preparation_context(&self) -> Self {
        let mut client = self.clone();
        client.preparation_context = None;
        client
    }

    #[cfg(test)]
    pub(crate) fn preparation_attribution_for_test(&self) -> Option<(u64, RpcRole, Option<u16>)> {
        self.preparation_context.as_ref().map(|attribution| {
            (
                attribution.context.attempt_id(),
                attribution.role,
                attribution.index,
            )
        })
    }

    /// Calls one JSON-RPC method and decodes its result.
    pub fn call<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<T, MinerError> {
        self.request(method, params, |result| {
            serde_json::from_value(result).map_err(|error| {
                MinerError::RpcProtocol(format!(
                    "{} returned an invalid {method} result: {error}",
                    self.label()
                ))
            })
        })
    }

    /// Calls one JSON-RPC method while preserving a JSON `null` result.
    pub fn call_value(&self, method: &'static str, params: Value) -> Result<Value, MinerError> {
        self.request(method, params, Ok)
    }

    fn request<T>(
        &self,
        method: &'static str,
        params: Value,
        decode: impl FnOnce(Value) -> Result<T, MinerError>,
    ) -> Result<T, MinerError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Ordinary RPC clients retain their prior path: no diagnostic clock,
        // phase read, event construction or queue access without an origin.
        let attribution = self
            .preparation_context
            .as_ref()
            .filter(|attribution| attribution.context.attempt_id() != 0);
        let started =
            attribution.map(|attribution| (Instant::now(), attribution.context.snapshot_phase()));
        let result = self.request_value(method, params, id).and_then(decode);
        if let (Err(error), Some(attribution), Some((started, phase))) =
            (&result, attribution, started)
        {
            attribution.context.record_failure(
                PreparationRpcFailure {
                    attempt_id: attribution.context.attempt_id(),
                    phase,
                    role: attribution.role,
                    recipient_index: attribution.index,
                    method: RpcMethod::from_name(method),
                    request_id: id,
                    elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    error_class: "rpc_failure",
                    rpc_error_code: None,
                    rpc_http_status: None,
                }
                .with_error(error),
            );
        }
        result
    }

    fn request_value(
        &self,
        method: &'static str,
        params: Value,
        id: u64,
    ) -> Result<Value, MinerError> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let mut builder = self
            .client
            .post(self.endpoint.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .json(&request);
        if let (Some(username), Some(password)) = (&self.endpoint.username, &self.endpoint.password)
        {
            builder = builder.basic_auth(username, Some(password));
        }

        let response = builder.send().map_err(MinerError::RpcTransport)?;
        let status = response.status();
        let response = read_bounded_response(response)?;
        classify_http_rpc_response(status, &response, id, self.label())
    }

    /// Starts a bounded ordinary GBT long poll so Zebra can prove and cache
    /// the next height's shielded coinbase before the current tip changes.
    pub fn prewarm_next_coinbase(
        &self,
        worker_index: usize,
        template_height: u32,
        long_poll_id: Option<String>,
    ) {
        let Some(mut long_poll_id) = long_poll_id.filter(|id| {
            !id.is_empty() && id.len() <= 1_024 && !id.bytes().any(|byte| byte.is_ascii_control())
        }) else {
            return;
        };
        let Some(prewarm_reservation) = self.reserve_coinbase_prewarm(template_height) else {
            return;
        };
        let endpoint = self.endpoint.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("coinbase-prewarm-{worker_index}"))
            .spawn(move || {
                let _prewarm_reservation = prewarm_reservation;
                // Keep one request armed for the whole 45-second generation.
                // A shorter ordinary RPC deadline creates periodic gaps where
                // a winner can arrive while Zebra is proving the replacement
                // coinbase again. Five extra seconds bound handoff overlap.
                let deadline = Instant::now() + Duration::from_secs(50);
                // The attempt cap also bounds a misbehaving endpoint that
                // repeatedly returns an immediately stale long-poll identity.
                for _ in 0..16 {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let Ok(client) = Self::new(endpoint.clone(), remaining) else {
                        break;
                    };
                    match client.call_value(
                        "getblocktemplate",
                        json!([{
                            "mode": "template",
                            "capabilities": ["coinbasetxn"],
                            "longpollid": long_poll_id,
                        }]),
                    ) {
                        Ok(response) => {
                            if response
                                .get("height")
                                .and_then(Value::as_u64)
                                .is_some_and(|height| height > u64::from(template_height))
                            {
                                break;
                            }

                            // A mempool-only change returns a same-height
                            // template. Rearm against its new identity while
                            // keeping the original generation deadline.
                            let Some(next_long_poll_id) = response
                                .get("longpollid")
                                .and_then(Value::as_str)
                                .filter(|id| {
                                    !id.is_empty()
                                        && id.len() <= 1_024
                                        && !id.bytes().any(|byte| byte.is_ascii_control())
                                })
                                .map(str::to_owned)
                            else {
                                break;
                            };
                            long_poll_id = next_long_poll_id;
                        }
                        Err(MinerError::RpcTransport(error)) if error.is_timeout() => break,
                        Err(_) => break,
                    }
                }
            });
        if let Err(error) = spawn_result {
            if crate::diagnostics::native_backend_mode() {
                crate::diagnostics::native_backend_prewarm_failure(worker_index, &error);
            } else {
                eprintln!("could not start coinbase prewarm worker {worker_index}: {error}");
            }
        }
    }

    fn reserve_coinbase_prewarm(&self, template_height: u32) -> Option<CoinbasePrewarmReservation> {
        let mut active_heights = self.active_coinbase_prewarms.lock().ok()?;
        if !active_heights.insert(template_height) {
            return None;
        }
        drop(active_heights);
        Some(CoinbasePrewarmReservation {
            active_heights: Arc::clone(&self.active_coinbase_prewarms),
            template_height,
        })
    }
}

fn read_bounded_response(mut response: Response) -> Result<Vec<u8>, MinerError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RPC_RESPONSE_BYTES_U64)
    {
        return Err(MinerError::RpcResponseTooLarge(MAX_RPC_RESPONSE_BYTES));
    }

    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_RPC_RESPONSE_BYTES_U64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_RPC_RESPONSE_BYTES {
        return Err(MinerError::RpcResponseTooLarge(MAX_RPC_RESPONSE_BYTES));
    }
    Ok(bytes)
}

fn classify_http_rpc_response(
    status: reqwest::StatusCode,
    bytes: &[u8],
    expected_id: u64,
    endpoint: &str,
) -> Result<Value, MinerError> {
    let parsed = parse_rpc_response(bytes, expected_id, endpoint);
    if status.is_success() {
        return parsed;
    }

    // Legacy zcashd transports ordinary JSON-RPC method errors as HTTP 500.
    // Preserve a correctly framed and ID-bound error object so callers can
    // distinguish an authoritative -5/not-found response from unavailability.
    match parsed {
        Err(error @ MinerError::RpcError { .. }) => Err(error),
        Ok(_) | Err(_) => Err(MinerError::RpcHttpStatus(status)),
    }
}

fn parse_rpc_response(bytes: &[u8], expected_id: u64, endpoint: &str) -> Result<Value, MinerError> {
    let response: Value = serde_json::from_slice(bytes)?;
    let object = response.as_object().ok_or_else(|| {
        MinerError::RpcProtocol(format!(
            "{endpoint} returned a non-object JSON-RPC response"
        ))
    })?;
    if object.get("id") != Some(&Value::from(expected_id)) {
        return Err(MinerError::RpcProtocol(format!(
            "{endpoint} returned a mismatched JSON-RPC id"
        )));
    }
    if let Some(version) = object.get("jsonrpc") {
        if version != "2.0" {
            return Err(MinerError::RpcProtocol(format!(
                "{endpoint} returned unsupported JSON-RPC version {version}"
            )));
        }
    }

    if let Some(error) = object.get("error").filter(|error| !error.is_null()) {
        let code = error.get("code").and_then(Value::as_i64);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown RPC error");
        return Err(MinerError::RpcError {
            endpoint: endpoint.to_string(),
            code,
            message: message.to_string(),
        });
    }

    object.get("result").cloned().ok_or_else(|| {
        MinerError::RpcProtocol(format!("{endpoint} response has neither result nor error"))
    })
}

fn is_loopback_host(url: &Url) -> bool {
    // Require a literal address rather than trusting DNS or a mutable hosts
    // file to keep a name such as `localhost` on the local machine.
    url.host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{preparation_timing::PreparationPhase, rpc_evidence::RpcRole};
    use std::{io::Write, net::TcpListener};

    fn evidence_rpc_fixture(
        status: u16,
        response: Value,
    ) -> (ZebraRpcClient, thread::JoinHandle<Value>) {
        evidence_rpc_fixture_with_gate(status, response, None)
    }

    fn evidence_rpc_fixture_with_gate(
        status: u16,
        mut response: Value,
        gate: Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    ) -> (ZebraRpcClient, thread::JoinHandle<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut request = Vec::new();
            let (header_end, body_len) = loop {
                let mut bytes = [0; 1_024];
                let read = stream.read(&mut bytes).unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&bytes[..read]);
                assert!(request.len() <= 65_536);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break (end + 4, length);
                    }
                }
            };
            let request: Value =
                serde_json::from_slice(&request[header_end..header_end + body_len]).unwrap();
            if let Some((entered, released)) = gate {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(3)).unwrap();
            }
            response["id"] = request["id"].clone();
            let body = serde_json::to_vec(&response).unwrap();
            write!(
                stream,
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
            request
        });
        let endpoint = RpcEndpoint::new(format!("http://{address}/"), None, None).unwrap();
        (
            ZebraRpcClient::new(endpoint, Duration::from_secs(3)).unwrap(),
            server,
        )
    }

    #[test]
    fn preparation_rpc_errors_keep_exact_variants_and_safe_source_attribution() {
        for (role, method, phase) in [
            (
                RpcRole::Wcash,
                "createauxblock",
                PreparationPhase::WcashCandidate,
            ),
            (
                RpcRole::Template,
                "getblocktemplate",
                PreparationPhase::ZcashTemplate,
            ),
            (
                RpcRole::Validator,
                "getblocktemplate",
                PreparationPhase::ZcashProposal,
            ),
        ] {
            let (client, server) = evidence_rpc_fixture(
                500,
                json!({"jsonrpc":"2.0","result":null,"error":{"code":-32001,"message":"PRIVATE_RPC_BODY"}}),
            );
            let (context, reports) = PreparationRpcContext::new().with_failure_capture();
            context.enter(phase);
            let client = client.with_preparation_context(role, 2, context.clone());
            let (error, events) =
                crate::diagnostics::capture(|| client.call_value(method, json!([])).unwrap_err());
            assert!(
                matches!(error, MinerError::RpcError { code: Some(-32001), ref message, .. } if message == "PRIVATE_RPC_BODY")
            );
            assert_eq!(server.join().unwrap()["method"], method);
            assert_eq!(events.len(), 1, "a failed request emits at most one record");
            let reports = reports.lock().unwrap();
            assert_eq!(reports.len(), 1);
            let report = reports[0];
            assert_eq!(report.attempt_id, context.attempt_id());
            assert_eq!(report.phase, phase);
            assert_eq!(report.role, role);
            assert_eq!(report.recipient_index, Some(2));
            assert_eq!(report.method, RpcMethod::from_name(method));
            assert_eq!(report.request_id, 1);
            assert_eq!(report.error_class, "rpc_error");
            assert_eq!(report.rpc_error_code, Some(-32001));
            assert_eq!(report.rpc_http_status, None);
            let debug = format!("{report:?}");
            assert!(!debug.contains("PRIVATE_RPC_BODY"));
            assert!(!debug.contains("127.0.0.1"));
        }
    }

    #[test]
    fn typed_result_failure_is_reported_once_after_decode() {
        let (client, server) = evidence_rpc_fixture(
            200,
            json!({"jsonrpc":"2.0","result":"PRIVATE_RESULT","error":null}),
        );
        let (context, reports) = PreparationRpcContext::new().with_failure_capture();
        let client = client.with_preparation_context(RpcRole::Wcash, 0, context);
        let (result, events) =
            crate::diagnostics::capture(|| client.call::<u32>("getblockcount", json!([])));
        assert!(matches!(result, Err(MinerError::RpcProtocol(_))));
        server.join().unwrap();
        assert_eq!(events.len(), 1);
        let reports = reports.lock().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].error_class, "rpc_response");
        assert!(!format!("{:?}", reports[0]).contains("PRIVATE_RESULT"));
    }

    #[test]
    fn unattributed_and_cleared_clients_do_not_emit_preparation_records() {
        for clear in [false, true] {
            let (client, server) =
                evidence_rpc_fixture(401, json!({"jsonrpc":"2.0","result":true,"error":null}));
            let (context, reports) = PreparationRpcContext::new().with_failure_capture();
            let client = if clear {
                client
                    .with_preparation_context(RpcRole::Template, 0, context)
                    .without_preparation_context()
            } else {
                client
            };
            let (result, events) =
                crate::diagnostics::capture(|| client.call_value("getblockcount", json!([])));
            assert!(matches!(
                result,
                Err(MinerError::RpcHttpStatus(reqwest::StatusCode::UNAUTHORIZED))
            ));
            server.join().unwrap();
            assert!(events.is_empty());
            assert!(reports.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn parallel_rpc_clones_retain_their_explicit_origin() {
        let mut fixtures = Vec::new();
        let (context, reports) = PreparationRpcContext::new().with_failure_capture();
        context.enter(PreparationPhase::CurrentnessFences);
        for index in 0..3 {
            let (client, server) = evidence_rpc_fixture(
                200,
                json!({"jsonrpc":"2.0","result":null,"error":{"code":-32001,"message":"PRIVATE_PARALLEL_BODY"}}),
            );
            fixtures.push((
                client.with_preparation_context(RpcRole::Validator, index, context.clone()),
                server,
            ));
        }
        thread::scope(|scope| {
            let requests: Vec<_> = fixtures
                .iter()
                .map(|(client, _)| scope.spawn(|| client.call_value("getbestblockhash", json!([]))))
                .collect();
            for request in requests {
                assert!(matches!(
                    request.join().unwrap(),
                    Err(MinerError::RpcError {
                        code: Some(-32001),
                        ..
                    })
                ));
            }
        });
        for (_, server) in fixtures {
            server.join().unwrap();
        }
        let mut reports = reports.lock().unwrap().clone();
        reports.sort_by_key(|report| report.recipient_index);
        assert_eq!(reports.len(), 3);
        for (index, report) in reports.into_iter().enumerate() {
            assert_eq!(report.attempt_id, context.attempt_id());
            assert_eq!(report.phase, PreparationPhase::CurrentnessFences);
            assert_eq!(report.recipient_index, Some(u16::try_from(index).unwrap()));
        }
    }

    #[test]
    fn preparation_phase_is_snapshotted_before_the_request() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let (client, server) = evidence_rpc_fixture_with_gate(
            200,
            json!({"jsonrpc":"2.0","result":null,"error":{"code":-1,"message":"error"}}),
            Some((entered_tx, release_rx)),
        );
        let (context, reports) = PreparationRpcContext::new().with_failure_capture();
        context.enter(PreparationPhase::ZcashProposal);
        let client =
            client.with_preparation_context(RpcRole::Validator, usize::MAX, context.clone());
        let request = thread::spawn(move || client.call_value("PRIVATE_UNKNOWN_METHOD", json!([])));
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        context.enter(PreparationPhase::Activation);
        release_tx.send(()).unwrap();
        assert!(matches!(
            request.join().unwrap(),
            Err(MinerError::RpcError { code: Some(-1), .. })
        ));
        server.join().unwrap();
        let report = reports.lock().unwrap()[0];
        assert_eq!(report.phase, PreparationPhase::ZcashProposal);
        assert_eq!(report.method, RpcMethod::Other);
        assert_eq!(report.recipient_index, None);
        assert_eq!(report.role, RpcRole::Unspecified);
    }

    #[test]
    fn endpoint_policy_never_exposes_credentials() {
        let endpoint = RpcEndpoint::new(
            "http://127.0.0.1:8232/",
            Some("rpc-user".to_string()),
            Some("rpc-secret".to_string()),
        )
        .expect("loopback HTTP is permitted");
        let debug = format!("{endpoint:?}");
        assert!(!debug.contains("rpc-user"));
        assert!(!debug.contains("rpc-secret"));
        assert!(RpcEndpoint::new("http://pool.example:8232", None, None).is_err());
        assert!(RpcEndpoint::new("http://localhost:8232", None, None).is_err());
        assert!(RpcEndpoint::new("https://pool.example:8232", None, None).is_ok());
        assert!(RpcEndpoint::new("https://pool.example:8232/private-token", None, None).is_err());
        assert!(RpcEndpoint::new("http://user:secret@127.0.0.1:8232", None, None).is_err());
        assert!(RpcEndpoint::new("ftp://127.0.0.1:8232", None, None).is_err());
        assert!(endpoint.is_loopback());
        assert!(!RpcEndpoint::new("https://pool.example:8232", None, None)
            .expect("remote HTTPS endpoint")
            .is_loopback());
    }

    #[test]
    fn rpc_envelope_is_id_bound_and_preserves_null() {
        assert!(matches!(
            parse_rpc_response(
                br#"{"jsonrpc":"2.0","id":7,"result":null,"error":null}"#,
                7,
                "node"
            ),
            Ok(Value::Null)
        ));
        assert!(matches!(
            parse_rpc_response(br#"{"jsonrpc":"2.0","id":8,"result":true}"#, 7, "node"),
            Err(MinerError::RpcProtocol(_))
        ));
        assert!(matches!(
            parse_rpc_response(
                br#"{"jsonrpc":"2.0","id":7,"result":null,"error":{"code":-1,"message":"denied"}}"#,
                7,
                "node"
            ),
            Err(MinerError::RpcError { code: Some(-1), .. })
        ));

        assert!(matches!(
            classify_http_rpc_response(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"jsonrpc":"2.0","id":7,"result":null,"error":{"code":-5,"message":"Block not found"}}"#,
                7,
                "node",
            ),
            Err(MinerError::RpcError { code: Some(-5), .. })
        ));
        assert!(matches!(
            classify_http_rpc_response(
                reqwest::StatusCode::UNAUTHORIZED,
                b"authentication required",
                7,
                "node",
            ),
            Err(MinerError::RpcHttpStatus(reqwest::StatusCode::UNAUTHORIZED))
        ));
    }

    #[test]
    fn coinbase_prewarms_are_deduplicated_per_endpoint_and_height() {
        let endpoint =
            RpcEndpoint::new("http://127.0.0.1:8232/", None, None).expect("loopback endpoint");
        let client = ZebraRpcClient::new(endpoint, Duration::from_secs(1))
            .expect("RPC client configuration is valid");
        let clone = client.clone();

        let height_10 = client
            .reserve_coinbase_prewarm(10)
            .expect("the first height reservation succeeds");
        assert!(
            clone.reserve_coinbase_prewarm(10).is_none(),
            "client clones must not start duplicate proofs for one height"
        );
        let height_11 = clone
            .reserve_coinbase_prewarm(11)
            .expect("different heights may briefly overlap during handoff");

        drop(height_10);
        assert!(client.reserve_coinbase_prewarm(10).is_some());
        drop(height_11);
    }
}
