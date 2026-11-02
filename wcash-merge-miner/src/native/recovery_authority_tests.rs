// Exact metadata protocol fixtures. Nodes attest metadata only; these tests do
// not emulate consensus fork choice, mine blocks, or contact public endpoints.

use std::sync::mpsc;

use super::*;

struct Call {
    method: &'static str,
    params: Value,
    reply: Option<Result<Value, Value>>,
    wait_for_release: bool,
}

struct Server {
    release: mpsc::Sender<()>,
    entered: mpsc::Receiver<()>,
    worker: Option<thread::JoinHandle<Vec<Value>>>,
}

impl Server {
    fn finish(mut self) -> Vec<Value> {
        let _ = self.release.send(());
        self.worker
            .take()
            .unwrap()
            .join()
            .expect("bounded metadata fixture")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.release.send(());
        if let Some(worker) = self.worker.take() {
            let joined = worker.join();
            if !thread::panicking() {
                joined.expect("metadata fixture cleanup");
            }
        }
    }
}

fn scripted(calls: Vec<Call>) -> (RpcEndpoint, Server) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = RpcEndpoint::new(
        format!("http://{}/", listener.local_addr().unwrap()),
        None,
        None,
    )
    .unwrap();
    let (release, release_rx) = mpsc::channel();
    let (entered_tx, entered) = mpsc::channel();
    let worker = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(4);
        let mut requests = Vec::new();
        for call in calls {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "metadata RPC is bounded");
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("metadata accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut content_length = None;
            let mut header_bytes = 0;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                header_bytes += line.len();
                assert!(header_bytes <= 8192);
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = Some(value.trim().parse::<usize>().unwrap());
                    }
                }
            }
            let length = content_length.unwrap();
            assert!(length <= 4096);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(request["method"], call.method);
            assert_eq!(request["params"], call.params);
            if call.wait_for_release {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("release timed-out metadata read");
            }
            if let Some(reply) = call.reply {
                let response = serde_json::to_vec(&match reply {
                    Ok(value) => json!({"jsonrpc":"2.0","id":request["id"],"result":value}),
                    Err(error) => json!({"jsonrpc":"2.0","id":request["id"],"error":error}),
                })
                .unwrap();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", response.len()).unwrap();
                stream.write_all(&response).unwrap();
            }
            requests.push(request);
        }
        requests
    });
    (
        endpoint,
        Server {
            release,
            entered,
            worker: Some(worker),
        },
    )
}

fn normal(role: usize, genesis: &str) -> Vec<Call> {
    let mut calls = vec![Call {
        method: "getblockhash",
        params: json!([0]),
        reply: Some(Ok(json!(genesis))),
        wait_for_release: false,
    }];
    if role < 2 {
        calls.push(Call {
            method: "getblockstatus",
            params: json!([genesis]),
            reply: Some(Ok(
                json!({"state":"best_chain","hash":genesis,"height":0,"confirmations":11}),
            )),
            wait_for_release: false,
        });
    }
    calls.push(Call {
        method: "getblockchaininfo",
        params: json!([]),
        reply: Some(Ok(json!({"blocks":10,"bestblockhash":"33".repeat(32)}))),
        wait_for_release: false,
    });
    calls
}

fn configuration(
    scripts: Vec<Vec<Call>>,
    network: NativeZcashNetwork,
    allow_lag: bool,
) -> (NativeZcashConfig, Vec<Server>) {
    assert_eq!(scripts.len(), 4);
    let (endpoints, servers): (Vec<_>, Vec<_>) = scripts.into_iter().map(scripted).unzip();
    let mut config = NativeZcashConfig::new(
        endpoints[0].clone(),
        vec![endpoints[1].clone()],
        network,
        network.consensus_parameters().genesis_hash().to_string(),
        "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV".parse().unwrap(),
    )
    .unwrap()
    .with_broadcast_nodes(vec![endpoints[2].clone()])
    .unwrap()
    .with_observer_nodes(vec![endpoints[3].clone()])
    .unwrap();
    if allow_lag {
        config = config
            .allow_lagging_proposal_validators_on_testnet()
            .unwrap();
    }
    (config, servers)
}

fn provider(
    scripts: Vec<Vec<Call>>,
    network: NativeZcashNetwork,
    allow_lag: bool,
) -> (NativeZcashProvider, Vec<Server>) {
    let (config, servers) = configuration(scripts, network, allow_lag);
    let (provider, _) = config.recovery_observation_provider().unwrap();
    (provider, servers)
}

fn finish(servers: Vec<Server>) -> Vec<Vec<Value>> {
    servers.into_iter().map(Server::finish).collect()
}

#[test]
fn recovery_reads_fresh_identity_status_capability_and_tip_for_every_configured_role() {
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    let scripts = (0..4).map(|role| normal(role, &genesis)).collect();
    let (provider, servers) = provider(scripts, network, false);
    let tip = provider.recovery_chain_tip().unwrap();
    assert_eq!(
        tip,
        NativeChainTip {
            block_hash_le: [0x33; 32],
            height: 10
        }
    );
    let transcripts = finish(servers);
    for (role, requests) in transcripts.iter().enumerate() {
        let methods = requests
            .iter()
            .map(|r| r["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            if role < 2 {
                vec!["getblockhash", "getblockstatus", "getblockchaininfo"]
            } else {
                vec!["getblockhash", "getblockchaininfo"]
            }
        );
    }
}

#[test]
fn a_network_identity_change_on_any_role_revokes_the_next_sample() {
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    for broken_role in 0..4 {
        let scripts = (0..4)
            .map(|role| {
                let mut calls = normal(role, &genesis);
                let mut second = normal(role, &genesis);
                if role == broken_role {
                    second.truncate(1);
                    second[0].reply = Some(Ok(json!("44".repeat(32))));
                }
                calls.extend(second);
                calls
            })
            .collect();
        let (provider, servers) = provider(scripts, network, false);
        provider.recovery_chain_tip().unwrap();
        assert!(matches!(
            provider.recovery_chain_tip(),
            Err(MinerError::NetworkIdentityMismatch { .. })
        ));
        finish(servers);
    }
}

#[test]
fn canonical_status_capability_is_mandatory_on_both_work_roles() {
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    for broken_role in 0..2 {
        for defect in [
            "unknown",
            "side_chain",
            "zero_confirmations",
            "wrong_hash",
            "wrong_height",
            "missing_api",
        ] {
            let scripts = (0..4).map(|role| {
                let mut calls = normal(role, &genesis);
                if role == broken_role {
                    calls.truncate(2);
                    calls[1].reply = Some(match defect {
                        "unknown" => Ok(json!({"state":"unknown"})),
                        "side_chain" => Ok(json!({"state":"side_chain","hash":genesis,"height":0})),
                        "zero_confirmations" => Ok(json!({"state":"best_chain","hash":genesis,"height":0,"confirmations":0})),
                        "wrong_hash" => Ok(json!({"state":"best_chain","hash":"55".repeat(32),"height":0,"confirmations":1})),
                        "wrong_height" => Ok(json!({"state":"best_chain","hash":genesis,"height":1,"confirmations":1})),
                        _ => Err(json!({"code":-32601,"message":"fixture method missing"})),
                    });
                }
                calls
            }).collect();
            let (provider, servers) = provider(scripts, network, false);
            assert!(
                provider.recovery_chain_tip().is_err(),
                "role={broken_role} defect={defect}"
            );
            finish(servers);
        }
    }
}

#[test]
fn no_configured_role_can_supply_forked_malformed_unavailable_or_partial_tip_evidence() {
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    for broken_role in 0..4 {
        for defect in [
            "same_height_fork",
            "height",
            "malformed",
            "rpc_unavailable",
            "transport",
        ] {
            let scripts = (0..4)
                .map(|role| {
                    let mut calls = normal(role, &genesis);
                    if role == broken_role {
                        calls.last_mut().unwrap().reply = match defect {
                            "same_height_fork" => {
                                Some(Ok(json!({"blocks":10,"bestblockhash":"66".repeat(32)})))
                            }
                            "height" => {
                                Some(Ok(json!({"blocks":9,"bestblockhash":"33".repeat(32)})))
                            }
                            "malformed" => Some(Ok(json!({"blocks":10,"bestblockhash":"invalid"}))),
                            "rpc_unavailable" => {
                                Some(Err(json!({"code":-28,"message":"fixture unavailable"})))
                            }
                            _ => None,
                        };
                    }
                    calls
                })
                .collect();
            let (provider, servers) = provider(scripts, network, false);
            assert!(
                provider.recovery_chain_tip().is_err(),
                "role={broken_role} defect={defect}"
            );
            finish(servers);
        }
    }
}

#[test]
fn stalled_metadata_read_is_bounded_independently_of_template_rpc_timeout() {
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    let scripts = (0..4)
        .map(|role| {
            let mut calls = normal(role, &genesis);
            if role == 3 {
                let tip = calls.last_mut().unwrap();
                tip.reply = None;
                tip.wait_for_release = true;
            }
            calls
        })
        .collect();
    let (provider, servers) = provider(scripts, network, false);
    let started = Instant::now();
    assert!(
        matches!(provider.recovery_chain_tip(), Err(MinerError::RpcTransport(error)) if error.is_timeout())
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    finish(servers);
}

#[test]
fn testnet_lagging_validator_opt_in_never_weakens_recovery_quorum() {
    let network = NativeZcashNetwork::Testnet;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    let scripts = (0..4)
        .map(|role| {
            let mut calls = normal(role, &genesis);
            if role == 1 {
                calls.last_mut().unwrap().reply =
                    Some(Ok(json!({"blocks":9,"bestblockhash":"77".repeat(32)})));
            }
            calls
        })
        .collect();
    let (provider, servers) = provider(scripts, network, true);
    assert!(provider.allow_lagging_proposal_validators);
    assert!(matches!(
        provider.recovery_chain_tip(),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    finish(servers);
}

#[test]
fn shared_zec_lane_refreshes_without_ordinary_job_and_shutdown_revokes_held_completion() {
    use crate::{
        pool_backend_supervision::NativePoolCurrentnessWorkers,
        zcash_recovery::{NativePoolZcashRecoveryAuthority, PoolBackendZcashWorkObservation},
    };
    let network = NativeZcashNetwork::Regtest;
    let genesis = network.consensus_parameters().genesis_hash().to_string();
    let scripts = (0..4)
        .map(|role| {
            let mut calls = normal(role, &genesis);
            let mut second = normal(role, &genesis);
            if role == 3 {
                second.last_mut().unwrap().wait_for_release = true;
            }
            calls.extend(second);
            calls
        })
        .collect();
    let (config, servers) = configuration(scripts, network, false);
    let authority = NativePoolZcashRecoveryAuthority::new(&config).unwrap();
    let expected = NativeChainTip {
        block_hash_le: [0x33; 32],
        height: 10,
    };
    assert_eq!(
        authority.observe_tip(expected),
        PoolBackendZcashWorkObservation::Unknown
    );
    let mut workers =
        NativePoolCurrentnessWorkers::spawn_with_zcash_recovery_authority(authority.clone())
            .unwrap();
    workers.tick(None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while authority.observe_tip(expected) != PoolBackendZcashWorkObservation::Current
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    let authenticated_without_job =
        authority.observe_tip(expected) == PoolBackendZcashWorkObservation::Current;
    // Publication can precede the first worker-result handoff. Keep polling
    // until that handoff is consumed and the one held second sample starts.
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut held = false;
    while Instant::now() < deadline {
        workers.generation_changed();
        workers.tick(None).unwrap();
        if servers[3].entered.try_recv().is_ok() {
            held = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let no_wcash_request = !workers.wcash_is_busy_for_test();
    let (ticked_tx, ticked_rx) = mpsc::channel();
    let tick_worker = thread::spawn(move || {
        let result = workers.tick(None);
        if ticked_tx.send((workers, result)).is_err() {
            panic!("controller result receiver must remain alive");
        }
    });
    let ticked = ticked_rx.recv_timeout(Duration::from_millis(500));
    let returned_while_held = ticked.is_ok();
    if ticked.is_err() {
        let _ = servers[3].release.send(());
    }
    let (mut workers, tick_result) =
        ticked.unwrap_or_else(|_| ticked_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    tick_worker.join().unwrap();
    tick_result.unwrap();
    let shutdown = thread::spawn(move || workers.request_shutdown());
    let deadline = Instant::now() + Duration::from_secs(1);
    while authority.observe_tip(expected) != PoolBackendZcashWorkObservation::Unknown
        && Instant::now() < deadline
    {
        thread::yield_now();
    }
    let revoked_before_release =
        authority.observe_tip(expected) == PoolBackendZcashWorkObservation::Unknown;
    // Release the still-valid positive metadata response after shutdown revoked authority.
    let _ = servers[3].release.send(());
    shutdown.join().unwrap().unwrap();
    let transcript = finish(servers);
    assert!(authenticated_without_job);
    assert!(
        held,
        "the second authenticated sample reached its held observer read"
    );
    assert!(no_wcash_request, "tick(None) never starts a WEC RPC lane");
    assert!(
        returned_while_held,
        "controller tick returns while metadata remains held"
    );
    assert!(revoked_before_release);
    assert_eq!(
        authority.observe_tip(expected),
        PoolBackendZcashWorkObservation::Unknown
    );
    for (role, calls) in transcript.iter().enumerate() {
        assert_eq!(calls.len(), if role < 2 { 6 } else { 4 });
        assert!(calls.iter().all(|call| matches!(
            call["method"].as_str(),
            Some("getblockhash" | "getblockstatus" | "getblockchaininfo")
        )));
    }
}
