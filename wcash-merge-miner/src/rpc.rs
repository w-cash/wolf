//! Bounded, credential-safe JSON-RPC transport for native Zcash nodes.

use std::{
    fmt,
    io::Read,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use reqwest::{
    blocking::{Client, Response},
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_TYPE},
    redirect::Policy,
    Url,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::MinerError;

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
        })
    }

    /// Returns the credential-free endpoint label.
    pub fn label(&self) -> &str {
        self.endpoint.label()
    }

    /// Calls one JSON-RPC method and decodes its result.
    pub fn call<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<T, MinerError> {
        let result = self.call_value(method, params)?;
        serde_json::from_value(result).map_err(|error| {
            MinerError::RpcProtocol(format!(
                "{} returned an invalid {method} result: {error}",
                self.label()
            ))
        })
    }

    /// Calls one JSON-RPC method while preserving a JSON `null` result.
    pub fn call_value(&self, method: &'static str, params: Value) -> Result<Value, MinerError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
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
}
