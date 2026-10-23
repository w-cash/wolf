//! Local RPC dispatch fixtures. These do not emulate consensus fork choice.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::{AtomicBool, Ordering},
};

use zcash_address::ToAddress;

use super::*;

struct FixtureState {
    tip: NativeChainTip,
    canonical: HashMap<u32, String>,
    blocks: HashMap<String, (u32, Block)>,
    membership: HashMap<String, ParentCommittedMembership>,
    observed: Block,
    accept_submission: bool,
    retain_submission_unknown: bool,
    submission_reply: Value,
    submission_error: Option<Value>,
    raw_override: Option<String>,
    tip_reads: usize,
    change_tip_on_read: Option<(usize, NativeChainTip)>,
    requests: Vec<Value>,
}

impl FixtureState {
    fn new(tip_block: Block, height: u32) -> Self {
        let genesis = public_block(0);
        let observed = public_block(1);
        let genesis_hash = display_hex(genesis.hash().0);
        let tip_hash = display_hex(tip_block.hash().0);
        let mut canonical = HashMap::from([(0, genesis_hash.clone())]);
        canonical.insert(height, tip_hash.clone());
        Self {
            tip: NativeChainTip {
                block_hash_le: tip_block.hash().0,
                height,
            },
            canonical,
            blocks: HashMap::from([
                (genesis_hash.clone(), (0, genesis)),
                (tip_hash.clone(), (height, tip_block)),
            ]),
            membership: HashMap::from([
                (genesis_hash, ParentCommittedMembership::BestChain),
                (tip_hash, ParentCommittedMembership::BestChain),
            ]),
            observed,
            accept_submission: true,
            retain_submission_unknown: false,
            submission_reply: Value::Null,
            submission_error: None,
            raw_override: None,
            tip_reads: 0,
            change_tip_on_read: None,
            requests: Vec::new(),
        }
    }

    fn mark_observed_side_chain(&mut self) {
        let hash = display_hex(self.observed.hash().0);
        self.blocks.insert(hash.clone(), (1, self.observed.clone()));
        self.membership
            .insert(hash, ParentCommittedMembership::SideChain);
    }

    fn dispatch(&mut self, request: &Value) -> Result<Value, Value> {
        self.requests.push(request.clone());
        assert!(self.requests.len() <= 64, "bounded fixture RPC count");
        let params = &request["params"];
        match request["method"].as_str().expect("RPC method") {
            "getblockchaininfo" => {
                self.tip_reads += 1;
                if let Some((read, changed)) = self.change_tip_on_read {
                    if self.tip_reads == read {
                        self.tip = changed;
                    }
                }
                Ok(json!({
                    "blocks": self.tip.height,
                    "bestblockhash": display_hex(self.tip.block_hash_le),
                }))
            }
            "getblockhash" => self
                .canonical
                .get(&u32::try_from(params[0].as_u64().expect("height")).unwrap())
                .map(|hash| json!(hash))
                .ok_or_else(|| json!({"code": -1, "message": "height unavailable"})),
            "getblockstatus" => {
                let hash = params[0].as_str().expect("exact block hash");
                Ok(match self.membership.get(hash) {
                    Some(ParentCommittedMembership::BestChain) => json!({
                        "state":"best_chain", "hash":hash,
                        "height":self.blocks[hash].0, "confirmations":1,
                    }),
                    Some(ParentCommittedMembership::SideChain) => json!({
                        "state":"side_chain", "hash":hash,
                        "height":self.blocks[hash].0,
                    }),
                    Some(ParentCommittedMembership::Unknown) | None => {
                        json!({"state":"unknown"})
                    }
                })
            }
            "getblock" | "getblockheader" => {
                let hash = params[0].as_str().expect("exact block hash");
                let Some((height, block)) = self.blocks.get(hash) else {
                    return Err(json!({"code":-5, "message":"block unavailable"}));
                };
                if request["method"] == "getblock" {
                    assert_eq!(params[1], 0, "relay requests exact raw bytes");
                    if let Some(raw) = &self.raw_override {
                        return Ok(json!(raw));
                    }
                    return Ok(json!(hex::encode(block.zcash_serialize_to_vec().unwrap())));
                }
                if params[1] == false {
                    return Ok(json!(hex::encode(
                        block.header.zcash_serialize_to_vec().unwrap()
                    )));
                }
                Ok(json!({
                    "hash":hash, "height":height,
                    "previousblockhash":display_hex(block.header.previous_block_hash.0),
                    "confirmations":if self.membership.get(hash)
                        == Some(&ParentCommittedMembership::BestChain) { 1 } else { -1 },
                }))
            }
            "submitblock" => {
                assert_eq!(params.as_array().unwrap().len(), 1);
                assert_eq!(
                    params[0],
                    hex::encode(self.observed.zcash_serialize_to_vec().unwrap()),
                    "the normal submission RPC must receive the exact observed public block"
                );
                let hash = display_hex(self.observed.hash().0);
                self.blocks.insert(hash.clone(), (1, self.observed.clone()));
                if self.accept_submission {
                    self.membership
                        .insert(hash.clone(), ParentCommittedMembership::BestChain);
                    self.canonical.insert(1, hash);
                    self.tip = NativeChainTip {
                        block_hash_le: self.observed.hash().0,
                        height: 1,
                    };
                } else if self.retain_submission_unknown {
                    self.blocks.remove(&hash);
                    self.membership.remove(&hash);
                } else {
                    // A valid side chain is deliberately not a convergence success.
                    self.membership
                        .insert(hash, ParentCommittedMembership::SideChain);
                }
                match &self.submission_error {
                    Some(error) => Err(error.clone()),
                    None => Ok(self.submission_reply.clone()),
                }
            }
            method => panic!("unexpected fixture method: {method}"),
        }
    }

    fn submissions(&self) -> usize {
        self.requests
            .iter()
            .filter(|request| request["method"] == "submitblock")
            .count()
    }

    fn raw_block_reads(&self) -> usize {
        self.requests
            .iter()
            .filter(|request| request["method"] == "getblock")
            .count()
    }
}

struct FixtureRpc {
    client: ZebraRpcClient,
    state: Arc<Mutex<FixtureState>>,
    stop: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
}

impl FixtureRpc {
    fn new(state: FixtureState) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local catch-up fixture");
        listener.set_nonblocking(true).unwrap();
        let endpoint = RpcEndpoint::new(
            format!("http://{}/", listener.local_addr().unwrap()),
            None,
            None,
        )
        .unwrap();
        let state = Arc::new(Mutex::new(state));
        let stop = Arc::new(AtomicBool::new(false));
        let server_state = Arc::clone(&state);
        let server_stop = Arc::clone(&stop);
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !server_stop.load(Ordering::Acquire) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_request(&mut stream);
                        let reply = server_state.lock().unwrap().dispatch(&request);
                        let response = serde_json::to_vec(&match reply {
                            Ok(result) => {
                                json!({"jsonrpc":"2.0", "id":request["id"], "result":result})
                            }
                            Err(error) => {
                                json!({"jsonrpc":"2.0", "id":request["id"], "error":error})
                            }
                        })
                        .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).unwrap();
                        stream.write_all(&response).unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("catch-up fixture accept failed: {error}"),
                }
            }
        });
        Self {
            client: ZebraRpcClient::new(endpoint, Duration::from_secs(2)).unwrap(),
            state,
            stop,
            server: Some(server),
        }
    }
}

impl Drop for FixtureRpc {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let joined = server.join();
            if !thread::panicking() {
                joined.expect("bounded catch-up RPC fixture completed");
            }
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Value {
    // macOS inherits O_NONBLOCK from the bounded accept listener.
    stream
        .set_nonblocking(false)
        .expect("blocking accepted fixture stream");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut content_length = None;
    let mut header_bytes = 0usize;
    loop {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        header_bytes += line.len();
        assert!(header_bytes <= 16_384, "bounded fixture headers");
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = Some(value.trim().parse::<usize>().unwrap());
            }
        }
    }
    let length = content_length.expect("bounded fixture RPC body");
    assert!(length <= 1_048_576);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn public_block(height: u32) -> Block {
    let encoded = match height {
        0 => include_str!("../../../zebra-test/src/vectors/block-main-0-000-000.txt"),
        1 => include_str!("../../../zebra-test/src/vectors/block-main-0-000-001.txt"),
        2 => include_str!("../../../zebra-test/src/vectors/block-main-0-000-002.txt"),
        _ => panic!("unsupported public fixture height"),
    };
    hex::decode(encoded.trim())
        .unwrap()
        .as_slice()
        .zcash_deserialize_into()
        .unwrap()
}

fn competing_fixture_block(different_predecessor: bool) -> Block {
    let mut block = public_block(1);
    // This models RPC observations only; the mutated header is not a valid
    // proof-of-work fixture and is never submitted or consensus-validated.
    Arc::make_mut(&mut block.header).nonce.0[0] ^= 1;
    if different_predecessor {
        Arc::make_mut(&mut block.header).previous_block_hash = block::Hash([0x44; 32]);
    }
    block
}

fn fixture_provider() -> NativeZcashProvider {
    let network = NativeZcashNetwork::Mainnet;
    let config = NativeZcashConfig::new(
        RpcEndpoint::new("http://127.0.0.1:1/", None, None).unwrap(),
        vec![RpcEndpoint::new("http://127.0.0.1:2/", None, None).unwrap()],
        network,
        network.consensus_parameters().genesis_hash().to_string(),
        ZcashAddress::from_transparent_p2pkh(NetworkType::Main, [0; 20]),
    )
    .unwrap();
    NativeZcashProvider::connect(config).unwrap()
}

fn observer_fixture() -> FixtureRpc {
    FixtureRpc::new(FixtureState::new(public_block(1), 1))
}

fn relay(
    provider: &NativeZcashProvider,
    observer: &FixtureRpc,
    node: &FixtureRpc,
) -> Result<(), MinerError> {
    let observed_tip = observer.state.lock().unwrap().tip;
    provider.relay_observer_blocks_to_node(&observer.client, observed_tip, &node.client)
}

#[test]
fn catchup_matching_tip_keeps_the_existing_fast_path() {
    let observer = observer_fixture();
    let node = FixtureRpc::new(FixtureState::new(public_block(1), 1));
    relay(&fixture_provider(), &observer, &node).unwrap();
    let state = node.state.lock().unwrap();
    assert_eq!(state.requests.len(), 1);
    assert_eq!(state.requests[0]["method"], "getblockchaininfo");
    assert!(observer.state.lock().unwrap().requests.is_empty());
}

#[test]
fn catchup_lower_tip_still_receives_exact_public_block() {
    let observer = observer_fixture();
    let node = FixtureRpc::new(FixtureState::new(public_block(0), 0));
    relay(&fixture_provider(), &observer, &node).unwrap();
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 1);
    assert_eq!(state.tip, observer.state.lock().unwrap().tip);
}

#[test]
fn catchup_same_height_unknown_sibling_relays_exact_public_block() {
    let observer = observer_fixture();
    let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 1));
    relay(&fixture_provider(), &observer, &node).unwrap();
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 1);
    assert_eq!(state.tip, observer.state.lock().unwrap().tip);
}

#[test]
fn catchup_same_height_different_predecessor_never_submits() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(true), 1);
    state.canonical.insert(0, "44".repeat(32));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_known_side_chain_is_not_resubmitted_or_forced() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.mark_observed_side_chain();
    let original_tip = state.tip;
    let node = FixtureRpc::new(state);
    let provider = fixture_provider();
    for _ in 0..2 {
        assert!(matches!(
            relay(&provider, &observer, &node),
            Err(MinerError::ParentTipMismatch { .. })
        ));
    }
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 0);
    assert_eq!(state.tip, original_tip);
}

#[test]
fn catchup_submission_side_chain_result_stays_a_mismatch_and_does_not_repeat() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.accept_submission = false;
    let original_tip = state.tip;
    let node = FixtureRpc::new(state);
    let provider = fixture_provider();
    for _ in 0..2 {
        assert!(matches!(
            relay(&provider, &observer, &node),
            Err(MinerError::ParentTipMismatch { .. })
        ));
    }
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 1);
    assert_eq!(state.tip, original_tip);
}

#[test]
fn catchup_node_ahead_is_never_forced_backwards() {
    let observer = observer_fixture();
    let node = FixtureRpc::new(FixtureState::new(public_block(2), 2));
    let original_tip = node.state.lock().unwrap().tip;
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 0);
    assert_eq!(state.tip, original_tip);
}

#[test]
fn catchup_ambiguous_pending_submission_is_paced_without_releasing_work() {
    for reply in [Value::Null, json!("duplicate"), json!("inconclusive")] {
        let observer = observer_fixture();
        let mut state = FixtureState::new(competing_fixture_block(false), 1);
        state.accept_submission = false;
        state.retain_submission_unknown = true;
        state.submission_reply = reply;
        let node = FixtureRpc::new(state);
        let provider = fixture_provider();
        for _ in 0..3 {
            assert!(matches!(
                relay(&provider, &observer, &node),
                Err(MinerError::ParentTipMismatch { .. })
            ));
        }
        assert_eq!(node.state.lock().unwrap().submissions(), 1);
        assert_eq!(observer.state.lock().unwrap().raw_block_reads(), 1);
        for last in provider
            .same_height_observer_relays
            .lock()
            .unwrap()
            .values_mut()
        {
            *last = Instant::now() - SAME_HEIGHT_OBSERVER_RELAY_INTERVAL;
        }
        assert!(matches!(
            relay(&provider, &observer, &node),
            Err(MinerError::ParentTipMismatch { .. })
        ));
        assert_eq!(node.state.lock().unwrap().submissions(), 2);
        assert_eq!(observer.state.lock().unwrap().raw_block_reads(), 2);
    }
}

#[test]
fn catchup_submit_rpc_error_after_canonical_commit_uses_authoritative_convergence() {
    for code in [0, -32603] {
        let observer = observer_fixture();
        let mut state = FixtureState::new(competing_fixture_block(false), 1);
        state.submission_error = Some(json!({
            "code":code, "message":"fixture gossip admission failed after commit",
        }));
        let node = FixtureRpc::new(state);
        relay(&fixture_provider(), &observer, &node).unwrap();
        assert_eq!(node.state.lock().unwrap().submissions(), 1);
        assert_eq!(
            node.state.lock().unwrap().tip,
            observer.state.lock().unwrap().tip,
        );
    }
}

#[test]
fn catchup_submit_rpc_error_without_convergence_stays_retryable_and_paced() {
    for code in [0, -32603] {
        let observer = observer_fixture();
        let mut state = FixtureState::new(competing_fixture_block(false), 1);
        state.accept_submission = false;
        state.retain_submission_unknown = true;
        state.submission_error = Some(json!({
            "code":code, "message":"fixture submission remains unknown",
        }));
        let node = FixtureRpc::new(state);
        let provider = fixture_provider();
        for _ in 0..3 {
            assert!(matches!(
                relay(&provider, &observer, &node),
                Err(MinerError::ParentTipMismatch { .. })
            ));
        }
        assert_eq!(node.state.lock().unwrap().submissions(), 1);
        assert_eq!(observer.state.lock().unwrap().raw_block_reads(), 1);
    }
}

#[test]
fn catchup_hard_submit_rpc_rejection_remains_hard_despite_tip_convergence() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.submission_error = Some(json!({
        "code":-22, "message":"fixture hard block rejection",
    }));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcError {
            code: Some(-22),
            ..
        })
    ));
    let state = node.state.lock().unwrap();
    assert_eq!(state.submissions(), 1);
    assert_eq!(state.tip, observer.state.lock().unwrap().tip);
}

#[test]
fn catchup_observer_change_before_write_defers_without_submission() {
    let mut state = FixtureState::new(public_block(1), 1);
    state.change_tip_on_read = Some((
        1,
        NativeChainTip {
            block_hash_le: public_block(2).hash().0,
            height: 2,
        },
    ));
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 1));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_recipient_change_before_write_defers_without_submission() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.change_tip_on_read = Some((
        2,
        NativeChainTip {
            block_hash_le: public_block(2).hash().0,
            height: 2,
        },
    ));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_recipient_already_converged_before_write_skips_submission() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.change_tip_on_read = Some((2, observer.state.lock().unwrap().tip));
    let node = FixtureRpc::new(state);
    relay(&fixture_provider(), &observer, &node).unwrap();
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_malformed_or_wrong_exact_raw_block_never_submits() {
    let valid = hex::encode(public_block(1).zcash_serialize_to_vec().unwrap());
    for raw in [
        "00".to_owned(),
        format!("{valid}00"),
        hex::encode(public_block(2).zcash_serialize_to_vec().unwrap()),
        "00".repeat(PARENT_BLOCK_LIMIT + 1),
    ] {
        let mut state = FixtureState::new(public_block(1), 1);
        state.raw_override = Some(raw);
        let observer = FixtureRpc::new(state);
        let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 1));
        let error = relay(&fixture_provider(), &observer, &node).unwrap_err();
        assert!(
            matches!(
                error,
                MinerError::RpcProtocol(_) | MinerError::InvalidParentTemplate(_)
            ),
            "raw-byte validation must reject the fixture, not a transport failure: {error:?}"
        );
        assert_eq!(observer.state.lock().unwrap().raw_block_reads(), 1);
        assert_eq!(node.state.lock().unwrap().submissions(), 0);
    }
}

#[test]
fn catchup_exact_hash_with_wrong_coinbase_height_never_submits() {
    let block = public_block(2);
    let parent = display_hex(block.header.previous_block_hash.0);
    let mut observer_state = FixtureState::new(block, 1);
    observer_state.canonical.insert(0, parent.clone());
    let observer = FixtureRpc::new(observer_state);
    let mut node_state = FixtureState::new(competing_fixture_block(false), 1);
    node_state.canonical.insert(0, parent);
    let node = FixtureRpc::new(node_state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcProtocol(_))
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_exact_hash_with_wrong_serialized_predecessor_never_submits() {
    let observer = FixtureRpc::new(FixtureState::new(competing_fixture_block(true), 1));
    let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 1));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcProtocol(_))
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn catchup_observer_change_after_write_never_reports_convergence() {
    let mut state = FixtureState::new(public_block(1), 1);
    state.change_tip_on_read = Some((
        2,
        NativeChainTip {
            block_hash_le: public_block(2).hash().0,
            height: 2,
        },
    ));
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 1));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
}

#[test]
fn catchup_recipient_change_after_write_never_reports_convergence() {
    let observer = observer_fixture();
    let mut state = FixtureState::new(competing_fixture_block(false), 1);
    state.change_tip_on_read = Some((
        3,
        NativeChainTip {
            block_hash_le: public_block(2).hash().0,
            height: 2,
        },
    ));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
}

#[test]
fn catchup_height_zero_conflict_never_fetches_or_submits_a_block() {
    let observer = FixtureRpc::new(FixtureState::new(public_block(0), 0));
    let node = FixtureRpc::new(FixtureState::new(competing_fixture_block(false), 0));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
    assert!(observer.state.lock().unwrap().requests.is_empty());
}

#[test]
fn catchup_deep_lower_tip_keeps_existing_bounded_relay_limit() {
    let mut state = FixtureState::new(public_block(1), MAX_OBSERVER_RELAY_BLOCKS + 1);
    state.tip.height = MAX_OBSERVER_RELAY_BLOCKS + 1;
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(FixtureState::new(public_block(0), 0));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
    assert!(observer.state.lock().unwrap().requests.is_empty());
}
