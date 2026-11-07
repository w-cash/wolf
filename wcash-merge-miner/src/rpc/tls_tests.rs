//! Real loopback TLS tests. All keys and credentials in this module are public fixtures.

use std::{
    error::Error,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    CertificateError, ServerConfig, ServerConnection,
};
use serde_json::{json, Value};

use super::{Client, HashSet, MinerError, Mutex, Policy, RpcEndpoint, ZebraRpcClient};

const SERVER_LIMIT: Duration = Duration::from_secs(6);
const REQUEST_LIMIT: usize = 8_192;
const USERNAME: &str = "public-test-user";
const PASSWORD: &str = "public-test-password";
const AUTHORIZATION: &str = "Basic cHVibGljLXRlc3QtdXNlcjpwdWJsaWMtdGVzdC1wYXNzd29yZA==";
const CA: &[u8] = include_bytes!("test-fixtures/public-test-ca.der");
const CERTIFICATE: &[u8] = include_bytes!("test-fixtures/public-test-server.der");
const PUBLIC_TEST_KEY: &[u8] = include_bytes!("test-fixtures/PUBLIC-TEST-KEY-DO-NOT-USE.pk8.der");

#[derive(Debug)]
struct Request {
    authorization: Option<String>,
    body: Value,
}

#[derive(Debug, Default)]
struct Observations {
    requests: Vec<Request>,
    rejected_handshakes: usize,
}

struct Server {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    result: Receiver<io::Result<Observations>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn start(connections: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback test listener binds");
        listener
            .set_nonblocking(true)
            .expect("loopback listener supports bounded nonblocking accepts");
        let address = listener
            .local_addr()
            .expect("bound listener has an address");
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let config = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .expect("test provider supports default TLS versions")
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(CERTIFICATE.to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(PUBLIC_TEST_KEY.to_vec())),
            )
            .expect("public fixture certificate matches its disposable test key");
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let (sender, result) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let outcome = serve(listener, Arc::new(config), connections, &worker_stop);
            let _ = sender.send(outcome);
        });
        Self {
            address,
            stop,
            result,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> Observations {
        let result = self
            .result
            .recv_timeout(SERVER_LIMIT + Duration::from_secs(1))
            .expect("bounded loopback TLS server finishes");
        let handle = self.thread.take().expect("server thread is owned once");
        let deadline = Instant::now() + Duration::from_secs(1);
        while !handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(handle.is_finished(), "server thread termination is bounded");
        handle.join().expect("loopback TLS server does not panic");
        result.expect("loopback TLS server completes its expected connections")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // A failed assertion or client construction must also release the server.
        // Every polling loop checks this flag and the original absolute deadline.
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn check_deadline(deadline: Instant, stop: &AtomicBool) -> io::Result<()> {
    if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "public loopback TLS fixture reached its deadline",
        ))
    } else {
        Ok(())
    }
}

fn serve(
    listener: TcpListener,
    config: Arc<ServerConfig>,
    connections: usize,
    stop: &AtomicBool,
) -> io::Result<Observations> {
    let deadline = Instant::now() + SERVER_LIMIT;
    let mut observations = Observations::default();
    for _ in 0..connections {
        let mut socket = loop {
            check_deadline(deadline, stop)?;
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(error),
            }
        };
        socket.set_nonblocking(true)?;
        let mut tls = ServerConnection::new(Arc::clone(&config)).map_err(io::Error::other)?;
        let mut rejected = false;
        while tls.is_handshaking() {
            check_deadline(deadline, stop)?;
            if pump(&mut tls, &mut socket).is_err() {
                observations.rejected_handshakes += 1;
                rejected = true;
                break;
            }
        }
        if rejected {
            continue;
        }
        let request = read_request(&mut tls, &mut socket, deadline, stop)?;
        let authenticated = request.authorization.as_deref() == Some(AUTHORIZATION);
        let (status, body) = if authenticated {
            (
                "200 OK",
                json!({"jsonrpc": "2.0", "id": request.body["id"], "result": 123}).to_string(),
            )
        } else {
            (
                "401 Unauthorized",
                "public test authentication rejected".into(),
            )
        };
        observations.requests.push(request);
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        tls.writer().write_all(response.as_bytes())?;
        tls.send_close_notify();
        while tls.wants_write() {
            check_deadline(deadline, stop)?;
            pump(&mut tls, &mut socket)?;
        }
    }
    Ok(observations)
}

fn pump(tls: &mut ServerConnection, socket: &mut TcpStream) -> io::Result<()> {
    match tls.complete_io(socket) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            thread::sleep(Duration::from_millis(1));
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn read_request(
    tls: &mut ServerConnection,
    socket: &mut TcpStream,
    deadline: Instant,
    stop: &AtomicBool,
) -> io::Result<Request> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1_024];
    loop {
        check_deadline(deadline, stop)?;
        match tls.reader().read(&mut buffer) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => bytes.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                pump(tls, socket)?;
                continue;
            }
            Err(error) => return Err(error),
        }
        if bytes.len() > REQUEST_LIMIT {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).map_err(io::Error::other)?;
        let header = |name: &str| {
            headers.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then(|| value.trim())
            })
        };
        let length = header("content-length")
            .ok_or(io::ErrorKind::InvalidData)?
            .parse::<usize>()
            .map_err(io::Error::other)?;
        if length > REQUEST_LIMIT {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let body = &bytes[header_end + 4..];
        if body.len() < length {
            continue;
        }
        return Ok(Request {
            authorization: header("authorization").map(str::to_owned),
            body: serde_json::from_slice(&body[..length]).map_err(io::Error::other)?,
        });
    }
}

fn client(server: &Server, host: &str, trust_ca: bool, password: Option<&str>) -> ZebraRpcClient {
    let endpoint = RpcEndpoint::new(
        format!("https://{host}:{}/", server.address.port()),
        password.map(|_| USERNAME.to_owned()),
        password.map(str::to_owned),
    )
    .expect("public test endpoint has a valid private credential configuration");
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .no_proxy()
        .redirect(Policy::none())
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        // Hostname mismatch tests also remain entirely on loopback, without DNS.
        .resolve(host, server.address);
    if trust_ca {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_der(CA).expect("public fixture CA is valid DER"),
        );
    }
    ZebraRpcClient {
        endpoint,
        client: builder.build().expect("bounded test TLS client builds"),
        request_timeout: Duration::from_secs(2),
        request_deadline: None,
        next_id: Arc::new(super::AtomicU64::new(1)),
        active_coinbase_prewarms: Arc::new(Mutex::new(HashSet::new())),
    }
}

fn certificate_rejection(error: &MinerError, expected: fn(&CertificateError) -> bool) {
    let MinerError::RpcTransport(error) = error else {
        panic!("expected a TLS transport rejection");
    };
    assert!(
        error.is_connect(),
        "certificate rejection occurs at connect"
    );
    let mut current: &(dyn Error + 'static) = error;
    loop {
        if let Some(rustls::Error::InvalidCertificate(certificate)) =
            current.downcast_ref::<rustls::Error>()
        {
            assert!(
                expected(certificate),
                "expected typed certificate rejection"
            );
            return;
        }
        // tokio-rustls keeps certificate errors in io::Error's custom inner
        // error, which get_ref preserves even if source() skips that wrapper.
        if let Some(inner) = current
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
        {
            current = inner;
            continue;
        }
        current = current
            .source()
            .expect("TLS rejection retains its typed certificate cause");
    }
}

#[test]
fn trusted_tls_transmits_exact_basic_auth_and_rejects_wrong_or_missing_credentials() {
    let server = Server::start(4);
    let good = client(&server, "127.0.0.1", true, Some(PASSWORD));
    assert_eq!(good.call_value("getblockcount", json!([])).unwrap(), 123);
    assert_eq!(
        good.clone().call_value("getblockcount", json!([])).unwrap(),
        123
    );
    for password in [Some("public-wrong-password"), None] {
        let bad = client(&server, "127.0.0.1", true, password);
        assert!(matches!(
            bad.call_value("getblockcount", json!([])),
            Err(MinerError::RpcHttpStatus(reqwest::StatusCode::UNAUTHORIZED))
        ));
    }
    let observed = server.finish();
    assert_eq!(observed.rejected_handshakes, 0);
    assert_eq!(observed.requests.len(), 4);
    for (request, id) in observed.requests[..2].iter().zip([1, 2]) {
        assert_eq!(request.authorization.as_deref(), Some(AUTHORIZATION));
        assert_eq!(
            request.body,
            json!({"jsonrpc": "2.0", "id": id, "method": "getblockcount", "params": []})
        );
    }
    assert_ne!(
        observed.requests[2].authorization.as_deref(),
        Some(AUTHORIZATION)
    );
    assert_eq!(observed.requests[3].authorization, None);
}

#[test]
fn untrusted_tls_ca_is_rejected_before_any_rpc_request() {
    let server = Server::start(1);
    let error = client(&server, "127.0.0.1", false, Some(PASSWORD))
        .call_value("getblockcount", json!([]))
        .expect_err("test CA is absent from this client's trust store");
    certificate_rejection(&error, |error| {
        matches!(error, CertificateError::UnknownIssuer)
    });
    let observed = server.finish();
    assert_eq!(observed.rejected_handshakes, 1);
    assert!(observed.requests.is_empty());
}

#[test]
fn wrong_tls_hostname_is_rejected_before_any_rpc_request() {
    let server = Server::start(1);
    let error = client(&server, "wrong-host.invalid", true, Some(PASSWORD))
        .call_value("getblockcount", json!([]))
        .expect_err("loopback certificate does not authenticate a different hostname");
    certificate_rejection(&error, |error| {
        matches!(
            error,
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
        )
    });
    let observed = server.finish();
    assert_eq!(observed.rejected_handshakes, 1);
    assert!(observed.requests.is_empty());
}
