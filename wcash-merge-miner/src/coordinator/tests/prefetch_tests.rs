use super::*;
use hex::ToHex;

use wcash_zec_parent::{
    address::ToAddress,
    chain::{
        block::{
            merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash,
        },
        parameters::{
            subsidy::{
                block_subsidy as parent_block_subsidy, funding_stream_address,
                funding_stream_values, miner_subsidy as parent_miner_subsidy,
                FundingStreamReceiver,
            },
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network as ParentNetwork, NetworkUpgrade as ParentNetworkUpgrade,
        },
        serialization::ZcashSerialize as ParentZcashSerialize,
        transaction::{LockTime as ParentLockTime, Transaction as ParentTransaction},
        transparent as parent_transparent,
    },
    NetworkType as ParentNetworkType, ZcashAddress as ParentAddress,
};

struct PrefetchRpcFixture {
    endpoint: RpcEndpoint,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    stopped: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl PrefetchRpcFixture {
    fn spawn(
        mut reply: impl FnMut(&serde_json::Value) -> Result<serde_json::Value, serde_json::Value>
            + Send
            + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = RpcEndpoint::new(
            format!("http://{}/", listener.local_addr().unwrap()),
            None,
            None,
        )
        .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_requests = Arc::clone(&requests);
        let worker_stopped = Arc::clone(&stopped);
        let worker = thread::spawn(move || {
            while !worker_stopped.load(Ordering::Acquire) {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("prefetch fixture accept failed: {error}"),
                };
                let request = read_test_rpc_request(&mut stream);
                worker_requests.lock().unwrap().push(request.clone());
                let response = match reply(&request) {
                    Ok(result) => json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                    Err(error) => json!({"jsonrpc":"2.0","id":request["id"],"error":error}),
                };
                let response = serde_json::to_vec(&response).unwrap();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            stopped,
            worker: Some(worker),
        }
    }

    fn requests(&self, method: &str) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == method)
            .cloned()
            .collect()
    }
}

impl Drop for PrefetchRpcFixture {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

fn prefetch_child_response(payout: [u8; 20], nonce: u8) -> serde_json::Value {
    let (mut candidate, _) = transparent_wcash_candidate(payout, 625_000_000);
    let header = Arc::make_mut(&mut candidate.header);
    header.version = WCASH_BLOCK_WIRE_VERSION;
    header.previous_block_hash = Network::new_wcash_regtest().genesis_hash();
    header.solution = Solution::for_wcash(vec![]).unwrap();
    header.nonce = [nonce; 32].into();
    Arc::make_mut(&mut candidate.header).merkle_root = candidate.transactions.iter().collect();
    let bits = candidate.header.difficulty_threshold;
    json!({
        "hash": candidate.hash().to_string(),
        "retiretoken": hex::encode([nonce; 32]),
        "data": hex::encode(candidate.zcash_serialize_to_vec().unwrap()),
        "chainid": WCASH_AUXILIARY_CHAIN_ID,
        "target": bits.to_expanded().unwrap().to_string(),
        "height": 1,
        "previousblockhash": candidate.header.previous_block_hash.to_string(),
        "bits": bits.to_string(),
        "coinbasevalue": 625_000_000i64,
    })
}

fn prefetch_child_rpc(
    payout: [u8; 20],
    retirement_failures: usize,
    acquisition_delay: Duration,
) -> (PrefetchRpcFixture, Arc<Mutex<String>>) {
    let tip = Arc::new(Mutex::new(
        Network::new_wcash_regtest().genesis_hash().to_string(),
    ));
    let server_tip = Arc::clone(&tip);
    let mut created = 0u8;
    let mut failed_retirements = retirement_failures;
    let server = PrefetchRpcFixture::spawn(move |request| {
        Ok(match request["method"].as_str().unwrap() {
            "getblockhash" => json!(Network::new_wcash_regtest().genesis_hash().to_string()),
            "getblockcount" => json!(0),
            "getbestblockhash" => json!(*server_tip.lock().unwrap()),
            "createauxblock" => {
                thread::sleep(acquisition_delay);
                created = created.checked_add(1).unwrap();
                prefetch_child_response(payout, created)
            }
            "retireauxblock" if failed_retirements > 0 => {
                failed_retirements -= 1;
                return Err(json!({"code":-32001,"message":"fixture retirement unavailable"}));
            }
            "retireauxblock" => json!({"state":"retired"}),
            _ => return Err(json!({"code":-32601,"message":"unexpected fixture method"})),
        })
    });
    (server, tip)
}

fn prefetch_parent_identity_rpc() -> PrefetchRpcFixture {
    let genesis = Network::new_regtest(Default::default())
        .genesis_hash()
        .to_string();
    PrefetchRpcFixture::spawn(move |request| {
        Ok(match request["method"].as_str().unwrap() {
            "getblockhash" => json!(genesis),
            "getblockstatus" => {
                json!({"state":"best_chain","hash":genesis,"height":0,"confirmations":1})
            }
            "getblockchaininfo" => json!({"blocks":0,"bestblockhash":genesis}),
            "getblocktemplate" => {
                return Err(json!({"code":-9,"message":"fixture parent template stop"}));
            }
            _ => return Err(json!({"code":-32601,"message":"unexpected parent fixture method"})),
        })
    })
}

fn prefetch_supervisor(
    directory: &Path,
    child: &PrefetchRpcFixture,
    parent: &PrefetchRpcFixture,
) -> (Arc<NativeMiningSupervisor>, PrefetchRpcFixture) {
    let validator = prefetch_parent_identity_rpc();
    let supervisor = Arc::new(test_pool_supervisor(
        directory,
        child.endpoint.clone(),
        parent.endpoint.clone(),
        validator.endpoint.clone(),
        vec![],
        vec![],
    ));
    (supervisor, validator)
}

#[test]
fn prefetched_child_is_validated_without_starting_a_parent_job_or_journal() {
    let directory = tempdir().unwrap();
    // Older nodes can ignore the extra argument. Their successful late reply
    // must still be fully consumed and its exact lease retained, not timed out.
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::from_millis(150));
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let started = Instant::now();
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let inner = child.child.as_ref().unwrap();
    assert!(inner.acquired_at >= started);
    assert_eq!(
        inner.ready_deadline,
        Some(inner.acquired_at + MAX_PREFETCHED_CHILD_AGE)
    );
    assert!(child.ready_age().unwrap() >= Duration::from_millis(150));
    assert!(child.is_fresh());
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
    assert!(!supervisor
        .initial_outbox_recovery_complete
        .load(Ordering::Acquire));
    assert!(parent_rpc.requests.lock().unwrap().is_empty());
    let expected_lease = inner.lease.clone();
    let redacted = format!("{child:?}");
    assert!(!redacted.contains(&expected_lease.retire_token));
    assert_eq!(child.retire().unwrap(), GenerationRetirement::Retired);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(
        child_rpc.requests("createauxblock")[0]["params"][1],
        json!({"mode":"speculative"})
    );
    assert_eq!(
        child_rpc.requests("retireauxblock")[0]["params"],
        json!([expected_lease.hash, expected_lease.retire_token])
    );
}

#[test]
fn prefetched_child_deadline_is_strict_and_never_restarts_on_inspection() {
    let directory = tempdir().unwrap();
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let inner = child.child.as_ref().unwrap();
    let original_deadline = inner.ready_deadline.unwrap();
    assert!(inner
        .ensure_fresh_at(original_deadline - Duration::from_nanos(1))
        .is_ok());
    for now in [
        original_deadline,
        original_deadline + Duration::from_secs(1),
    ] {
        assert!(matches!(
            inner.ensure_fresh_at(now),
            Err(MinerError::PrefetchedChildExpired)
        ));
    }
    assert!(child.is_fresh());
    assert!(child.ready_age().is_some());
    assert_eq!(
        child.child.as_ref().unwrap().ready_deadline,
        Some(original_deadline)
    );
    child.retire().unwrap();
}

#[test]
fn expired_prefetched_child_retires_without_contacting_the_parent_or_reacquiring() {
    let directory = tempdir().unwrap();
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let exact_lease = child.child.as_ref().unwrap().lease.clone();
    child.child.as_mut().unwrap().ready_deadline = Some(Instant::now());
    assert!(!child.is_fresh());
    assert!(matches!(
        child.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::PrefetchedChildExpired)
    ));
    assert!(parent_rpc.requests.lock().unwrap().is_empty());
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(
        child_rpc.requests("retireauxblock")[0]["params"],
        json!([exact_lease.hash, exact_lease.retire_token])
    );
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn same_height_child_reorg_rejects_prefetched_work_before_parent_construction() {
    let directory = tempdir().unwrap();
    let (child_rpc, tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    *tip.lock().unwrap() = "77".repeat(32);
    assert!(matches!(
        child.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::ChildTipMismatch { .. })
    ));
    assert!(parent_rpc.requests("getblocktemplate").is_empty());
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.next_auxiliary_nonce.load(Ordering::Acquire), 0);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn prefetched_child_ready_hit_never_reacquires_and_parent_failure_retires_it() {
    let directory = tempdir().unwrap();
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    assert!(matches!(
        child.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::RpcError { code: Some(-9), .. })
    ));
    assert_eq!(parent_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn prefetched_child_failed_retirement_retains_its_exact_owner_until_retry() {
    let directory = tempdir().unwrap();
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 1, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let lease = child.child.as_ref().unwrap().lease.clone();
    assert!(child.retire().is_err());
    assert_eq!(child.child.as_ref().unwrap().lease, lease);
    assert!(child.is_fresh());
    assert_eq!(child.retire().unwrap(), GenerationRetirement::Retired);
    assert!(child.child.is_none());
    assert_eq!(child.retire().unwrap(), GenerationRetirement::Retired);
    let requests = child_rpc.requests("retireauxblock");
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(request["params"], json!([lease.hash, lease.retire_token]));
    }
}

#[derive(Clone, Copy)]
enum ParentRetryAction {
    Accept,
    AcceptChildReorg,
    Advance,
    Reorg,
    ReorgChild,
    ReorgPayout,
    UnprovenRejection,
}

struct ParentRetryTip {
    height: u32,
    hash: String,
    corrupt_payout: bool,
}

fn parent_retry_address() -> ParentAddress {
    ParentAddress::from_transparent_p2pkh(ParentNetworkType::Regtest, [0x11; 20])
}

// These canonical, unsolved templates exercise the real native parser and RPC
// proposal gates. The fixture validator supplies responses; it is not a node
// consensus test and does not establish that a solved block would be accepted.
fn parent_retry_template(request: &serde_json::Value, tip: &ParentRetryTip) -> serde_json::Value {
    let network = ParentNetwork::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let height = wcash_zec_parent::chain::block::Height(tip.height + 1);
    let subsidy = parent_block_subsidy(height, &network).unwrap();
    let reward = parent_miner_subsidy(height, &network, subsidy).unwrap();
    let script =
        parent_transparent::Address::from_pub_key_hash(network.kind(), [0x11; 20]).script();
    let mut outputs = vec![parent_transparent::Output::new(reward, script)];
    for (receiver, value) in funding_stream_values(height, &network, subsidy).unwrap() {
        if receiver != FundingStreamReceiver::Deferred {
            let address = funding_stream_address(height, &network, receiver).unwrap();
            outputs.push(parent_transparent::Output::new(value, address.script()));
        }
    }
    let child = parse_display_hash(
        request["params"][0]["wcashaux"]["blockhash"]
            .as_str()
            .unwrap(),
        "fixture child hash",
    )
    .unwrap();
    let nonce = u32::try_from(request["params"][0]["wcashaux"]["nonce"].as_u64().unwrap()).unwrap();
    let coinbase = ParentTransaction::test_v5(
        ParentNetworkUpgrade::Nu6_3,
        vec![parent_transparent::Input::Coinbase {
            height,
            data: wcash_zcash_aux::miner_data_commitment(child, &[], 0, nonce)
                .unwrap()
                .to_vec(),
            sequence: u32::MAX,
        }],
        outputs,
        ParentLockTime::unlocked(),
        height,
    );
    let transactions = [Arc::new(coinbase.clone())];
    let merkle: wcash_zec_parent::chain::block::merkle::Root = transactions.iter().collect();
    let auth: AuthDataRoot = transactions.iter().collect();
    let history: ChainHistoryMmrRootHash = [0x22; 32].into();
    let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(&history, &auth);
    let mut template: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../zebra-rpc/tests/vectors/getblocktemplate_response_template.json"
    ))
    .unwrap();
    template["height"] = json!(height.0);
    template["previousblockhash"] = json!(tip.hash);
    template["transactions"] = json!([]);
    template["coinbasetxn"] = json!({
        "data": hex::encode(coinbase.zcash_serialize_to_vec().unwrap()),
        "hash": coinbase.hash().encode_hex::<String>(),
        "authdigest": coinbase.auth_digest().unwrap().encode_hex::<String>(),
        "fee": 0,
    });
    template["defaultroots"] = json!({
        "merkleroot": merkle.encode_hex::<String>(),
        "authdataroot": auth.encode_hex::<String>(),
        "chainhistoryroot": history.encode_hex::<String>(),
        "blockcommitmentshash": commitment.encode_hex::<String>(),
    });
    template["blockcommitmentshash"] = json!(commitment.encode_hex::<String>());
    template["wcashparentpayoutcommitment"] = json!(hex::encode(
        wcash_zcash_aux::parent_payout_address_commitment(&parent_retry_address().to_string())
    ));
    if tip.corrupt_payout {
        template["wcashparentpayoutcommitment"] = json!("00".repeat(32));
    }
    template["longpollid"] = serde_json::Value::Null;
    template
}

fn parent_retry_rpcs(
    actions: Vec<ParentRetryAction>,
    child_tip: Arc<Mutex<String>>,
) -> (
    PrefetchRpcFixture,
    PrefetchRpcFixture,
    Arc<Mutex<ParentRetryTip>>,
) {
    let genesis = Network::new_regtest(Default::default())
        .genesis_hash()
        .to_string();
    let tip = Arc::new(Mutex::new(ParentRetryTip {
        height: 0,
        hash: genesis.clone(),
        corrupt_payout: false,
    }));
    let template_tip = Arc::clone(&tip);
    let template_genesis = genesis.clone();
    let template = PrefetchRpcFixture::spawn(move |request| {
        Ok(match request["method"].as_str().unwrap() {
            "getblockhash" => json!(template_genesis),
            "getblockstatus" => json!({
                "state":"best_chain", "hash":template_genesis, "height":0, "confirmations":1
            }),
            "getblockchaininfo" => {
                let tip = template_tip.lock().unwrap();
                json!({"blocks":tip.height,"bestblockhash":tip.hash})
            }
            "getblocktemplate" => {
                assert_eq!(request["params"][0]["mode"], "template");
                parent_retry_template(request, &template_tip.lock().unwrap())
            }
            _ => return Err(json!({"code":-32601,"message":"unexpected template fixture method"})),
        })
    });
    let validator_tip = Arc::clone(&tip);
    let mut actions = std::collections::VecDeque::from(actions);
    let validator = PrefetchRpcFixture::spawn(move |request| {
        Ok(match request["method"].as_str().unwrap() {
            "getblockhash" => json!(genesis),
            "getblockstatus" => json!({
                "state":"best_chain", "hash":genesis, "height":0, "confirmations":1
            }),
            "getblockchaininfo" => {
                let tip = validator_tip.lock().unwrap();
                json!({"blocks":tip.height,"bestblockhash":tip.hash})
            }
            "getblocktemplate" => {
                assert_eq!(request["params"][0]["mode"], "proposal");
                let Some(action) = actions.pop_front() else {
                    return Err(
                        json!({"code":-32603,"message":"parent retry exceeded fixture bound"}),
                    );
                };
                match action {
                    ParentRetryAction::Accept => serde_json::Value::Null,
                    ParentRetryAction::AcceptChildReorg => {
                        *child_tip.lock().unwrap() = "77".repeat(32);
                        serde_json::Value::Null
                    }
                    ParentRetryAction::UnprovenRejection => {
                        json!("proposal-is-not-based-on-the-current-best-chain-tip")
                    }
                    ParentRetryAction::Advance
                    | ParentRetryAction::Reorg
                    | ParentRetryAction::ReorgChild
                    | ParentRetryAction::ReorgPayout => {
                        let mut tip = validator_tip.lock().unwrap();
                        if matches!(action, ParentRetryAction::Advance) {
                            tip.height += 1;
                        }
                        tip.hash = if tip.hash == "32".repeat(32) {
                            "33".repeat(32)
                        } else {
                            "32".repeat(32)
                        };
                        if matches!(action, ParentRetryAction::ReorgChild) {
                            *child_tip.lock().unwrap() = "77".repeat(32);
                        }
                        if matches!(action, ParentRetryAction::ReorgPayout) {
                            tip.corrupt_payout = true;
                        }
                        json!("proposal-is-not-based-on-the-current-best-chain-tip")
                    }
                }
            }
            _ => {
                return Err(json!({"code":-32601,"message":"unexpected validator fixture method"}))
            }
        })
    });
    (template, validator, tip)
}

fn parent_retry_supervisor(
    directory: &Path,
    child: &PrefetchRpcFixture,
    template: &PrefetchRpcFixture,
    validator: &PrefetchRpcFixture,
) -> Arc<NativeMiningSupervisor> {
    let (_, payout) = transparent_wcash_candidate([0x71; 20], 625_000_000);
    Arc::new(
        NativeMiningSupervisor::open(
            CoordinatorConfig {
                wcash_node: child.endpoint.clone(),
                expected_wcash_genesis_hash: Network::new_wcash_regtest()
                    .genesis_hash()
                    .to_string(),
                zcash: NativeZcashConfig::new(
                    template.endpoint.clone(),
                    vec![validator.endpoint.clone()],
                    NativeZcashNetwork::Regtest,
                    Network::new_regtest(Default::default())
                        .genesis_hash()
                        .to_string(),
                    parent_retry_address().to_string().parse().unwrap(),
                )
                .unwrap(),
                wcash_payout_address: payout.encode(),
                wcash_payout_incoming_viewing_key: None,
                auxiliary_nonce: 0,
            },
            directory.join("native-share-journal.jsonl"),
        )
        .unwrap(),
    )
}

#[test]
fn parent_tip_race_retries_one_full_native_parent_without_reacquiring_the_child() {
    for race in [ParentRetryAction::Advance, ParentRetryAction::Reorg] {
        for prefetched in [false, true] {
            let directory = tempdir().unwrap();
            let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
            let (template_rpc, validator_rpc, parent_tip) =
                parent_retry_rpcs(vec![race, ParentRetryAction::Accept], child_tip);
            let supervisor = parent_retry_supervisor(
                directory.path(),
                &child_rpc,
                &template_rpc,
                &validator_rpc,
            );
            let mut timing = NativePreparationTiming::new();
            let mut generation = if prefetched {
                supervisor
                    .prepare_child_candidate_timed(&mut timing)
                    .unwrap()
                    .prepare_generation_timed(&mut timing)
            } else {
                supervisor.prepare_generation_timed(&mut timing)
            }
            .expect("one proven parent race reuses the already validated child");
            let templates = template_rpc.requests("getblocktemplate");
            let proposals = validator_rpc.requests("getblocktemplate");
            assert_eq!(child_rpc.requests("createauxblock").len(), 1);
            assert!(child_rpc.requests("retireauxblock").is_empty());
            assert_eq!(templates.len(), 2);
            assert_eq!(proposals.len(), 2);
            assert_eq!(templates[0]["params"][0]["wcashaux"]["nonce"], 0);
            assert_eq!(templates[1]["params"][0]["wcashaux"]["nonce"], 1);
            assert_eq!(
                templates[0]["params"][0]["wcashaux"]["blockhash"],
                templates[1]["params"][0]["wcashaux"]["blockhash"]
            );
            assert_ne!(
                proposals[0]["params"][0]["data"],
                proposals[1]["params"][0]["data"]
            );
            assert_eq!(
                generation.job().parent_tip_display(),
                parent_tip.lock().unwrap().hash
            );
            assert_eq!(supervisor.journal.generation_count().unwrap(), 1);
            assert_eq!(supervisor.next_auxiliary_nonce.load(Ordering::Acquire), 2);
            assert_eq!(
                generation.retire_generation().unwrap(),
                GenerationRetirement::Retired
            );
            assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
        }
    }
}

#[test]
fn second_parent_tip_race_retires_the_same_child_without_an_unbounded_retry() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) = parent_retry_rpcs(
        vec![ParentRetryAction::Advance, ParentRetryAction::Reorg],
        child_tip,
    );
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    let child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let lease = child.child.as_ref().unwrap().lease.clone();
    assert!(matches!(
        child.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 2);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 2);
    let retirements = child_rpc.requests("retireauxblock");
    assert_eq!(retirements.len(), 1);
    assert_eq!(
        retirements[0]["params"],
        json!([lease.hash, lease.retire_token])
    );
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn parent_retry_rechecks_same_height_child_reorg_before_constructing_another_parent() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) =
        parent_retry_rpcs(vec![ParentRetryAction::ReorgChild], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    assert!(matches!(
        supervisor.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::ChildTipMismatch { .. })
    ));
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn parent_proposal_rejection_without_a_proven_tip_change_never_retries() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) =
        parent_retry_rpcs(vec![ParentRetryAction::UnprovenRejection], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    assert!(matches!(
        supervisor.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::ParentProposalRejected { .. })
    ));
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn parent_retry_revalidates_the_parent_payout_commitment_before_a_second_proposal() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) =
        parent_retry_rpcs(vec![ParentRetryAction::ReorgPayout], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    assert!(matches!(
        supervisor.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::InvalidParentTemplate(message))
            if message.contains("payout commitment")
    ));
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 2);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn original_baseline_acquisition_deadline_stops_retry_and_retires_the_exact_child() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) =
        parent_retry_rpcs(vec![ParentRetryAction::Advance], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let mut prepared = *child.child.take().unwrap();
    let lease = prepared.lease.clone();
    // Model baseline acquisition that consumed its entire retry lifetime. An
    // original baseline attempt is permitted; an immediate retry is not.
    prepared.acquired_at = Instant::now() - MAX_PREFETCHED_CHILD_AGE;
    prepared.ready_deadline = None;
    assert!(matches!(
        supervisor.prepare_generation_with_child_timed(
            Some(prepared),
            &mut NativePreparationTiming::new(),
        ),
        Err(MinerError::PrefetchedChildExpired)
    ));
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    let retirements = child_rpc.requests("retireauxblock");
    assert_eq!(retirements.len(), 1);
    assert_eq!(
        retirements[0]["params"],
        json!([lease.hash, lease.retire_token])
    );
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn parent_retry_keeps_the_final_child_fence_after_the_second_independent_proposal() {
    let directory = tempdir().unwrap();
    let (child_rpc, child_tip) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let (template_rpc, validator_rpc, _) = parent_retry_rpcs(
        vec![
            ParentRetryAction::Advance,
            ParentRetryAction::AcceptChildReorg,
        ],
        child_tip,
    );
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    assert!(matches!(
        supervisor.prepare_generation_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::ChildTipMismatch { .. })
    ));
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 2);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 2);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
}

#[test]
fn parent_retry_deadline_preserves_the_original_acquisition_and_prefetch_bounds() {
    let directory = tempdir().unwrap();
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::ZERO);
    let parent_rpc = prefetch_parent_identity_rpc();
    let (supervisor, _validator_rpc) =
        prefetch_supervisor(directory.path(), &child_rpc, &parent_rpc);
    let mut child = supervisor
        .prepare_child_candidate_timed(&mut NativePreparationTiming::new())
        .unwrap();
    let prepared = child.child.as_mut().unwrap();
    let original_acquired_at = prepared.acquired_at;
    let acquisition_deadline = original_acquired_at + MAX_PREFETCHED_CHILD_AGE;
    let original_ready_deadline = prepared.ready_deadline;
    assert!(prepared
        .ensure_parent_retry_fresh_at(acquisition_deadline - Duration::from_nanos(1))
        .is_ok());
    assert!(matches!(
        prepared.ensure_parent_retry_fresh_at(acquisition_deadline),
        Err(MinerError::PrefetchedChildExpired)
    ));
    // Baseline children have no ready-cache deadline, but a parent retry cannot
    // acquire a fresh lifetime by removing that optional prefetch marker.
    prepared.ready_deadline = None;
    assert!(matches!(
        prepared.ensure_parent_retry_fresh_at(acquisition_deadline),
        Err(MinerError::PrefetchedChildExpired)
    ));
    prepared.ready_deadline = original_ready_deadline;
    assert_eq!(prepared.acquired_at, original_acquired_at);
    assert_eq!(prepared.ready_deadline, original_ready_deadline);
    assert_eq!(child_rpc.requests("createauxblock").len(), 1);
    assert!(parent_rpc.requests("getblocktemplate").is_empty());
    child.retire().unwrap();
}

#[test]
fn parent_retry_publication_deadline_is_strict_without_renewing_the_original_bound() {
    let deadline = Instant::now() + MAX_PREFETCHED_CHILD_AGE;
    assert!(ensure_parent_retry_deadline_at(None, deadline).is_ok());
    assert!(
        ensure_parent_retry_deadline_at(Some(deadline), deadline - Duration::from_nanos(1),)
            .is_ok()
    );
    for now in [deadline, deadline + Duration::from_nanos(1)] {
        assert!(matches!(
            ensure_parent_retry_deadline_at(Some(deadline), now),
            Err(MinerError::PrefetchedChildExpired)
        ));
    }
}

// This fixture models the node's pre-publication wait boundary, not proving or consensus.
// The node's real-cache fixtures independently check that a pending reply allocates no lease.
struct SpeculativeWaitGate(Arc<(Mutex<bool>, std::sync::Condvar)>);

impl SpeculativeWaitGate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), std::sync::Condvar::new())))
    }

    fn release(&self) {
        let (released, changed) = &*self.0;
        *released.lock().unwrap() = true;
        changed.notify_all();
    }
}

impl Drop for SpeculativeWaitGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn speculative_wait_fixture(
    gate: &SpeculativeWaitGate,
    warm_required_key: Arc<AtomicBool>,
) -> (
    PrefetchRpcFixture,
    Arc<Mutex<String>>,
    std::sync::mpsc::Receiver<()>,
) {
    let tip = Arc::new(Mutex::new(
        Network::new_wcash_regtest().genesis_hash().to_string(),
    ));
    let server_tip = Arc::clone(&tip);
    let proof_gate = Arc::clone(&gate.0);
    let mut allocated = 0u8;
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let fixture = PrefetchRpcFixture::spawn(move |request| {
        Ok(match request["method"].as_str().unwrap() {
            "getblockhash" => json!(Network::new_wcash_regtest().genesis_hash().to_string()),
            "getblockcount" => json!(0),
            "getbestblockhash" => json!(*server_tip.lock().unwrap()),
            "createauxblock" => {
                let (released, changed) = &*proof_gate;
                if !warm_required_key.load(Ordering::Acquire) && !*released.lock().unwrap() {
                    if request["params"].get(1) == Some(&json!({"mode":"speculative"})) {
                        // No candidate or secret retirement token exists on this return path.
                        return Err(
                            json!({"code":-32001,"message":"Wcash candidate construction is pending; retry createauxblock"}),
                        );
                    }
                    let _ = held_tx.send(());
                    let (released, timeout) = changed
                        .wait_timeout_while(
                            released.lock().unwrap(),
                            Duration::from_secs(3),
                            |released| !*released,
                        )
                        .unwrap();
                    if !*released || timeout.timed_out() {
                        return Err(json!({"code":-9,"message":"fixture proof gate deadline"}));
                    }
                }
                allocated = allocated.checked_add(1).unwrap();
                prefetch_child_response([0x71; 20], allocated)
            }
            "retireauxblock" => json!({"state":"retired"}),
            _ => {
                return Err(
                    json!({"code":-32601,"message":"unexpected speculative fixture method"}),
                )
            }
        })
    });
    (fixture, tip, held_rx)
}

#[test]
fn obsolete_speculative_wait_yields_to_a_warm_required_key_before_old_proof_finishes() {
    let directory = tempdir().unwrap();
    let gate = SpeculativeWaitGate::new();
    let warm_key = Arc::new(AtomicBool::new(false));
    let (child_rpc, child_tip, _held) = speculative_wait_fixture(&gate, Arc::clone(&warm_key));
    let (template_rpc, validator_rpc, parent_tip) =
        parent_retry_rpcs(vec![ParentRetryAction::Accept], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    let preparing = Arc::clone(&supervisor);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        result_tx
            .send(preparing.prepare_child_candidate_timed(&mut NativePreparationTiming::new()))
            .unwrap();
    });
    let result = result_rx.recv_timeout(Duration::from_millis(500));
    if result.is_err() {
        // Baseline red disposes of its owned RPC result before reporting the failure.
        gate.release();
        worker.join().unwrap();
        if let Ok(Ok(mut child)) = result_rx.try_recv() {
            child.retire().unwrap();
        }
        panic!("speculative pre-publication wait held the only preparation worker");
    }
    worker.join().unwrap();
    assert!(matches!(
        result.unwrap(),
        Err(MinerError::RpcError {
            code: Some(-32001),
            ..
        })
    ));
    assert_eq!(supervisor.journal.generation_count().unwrap(), 0);
    assert!(child_rpc.requests("retireauxblock").is_empty());

    // The held proof belongs to an obsolete selection; the new required exact key is ready.
    warm_key.store(true, Ordering::Release);
    {
        let mut tip = parent_tip.lock().unwrap();
        tip.height = 1;
        tip.hash = "32".repeat(32);
    }
    let mut generation = supervisor
        .prepare_generation_timed(&mut NativePreparationTiming::new())
        .unwrap();
    assert!(
        !*gate.0 .0.lock().unwrap(),
        "fresh parent must not wait for the obsolete proof"
    );
    assert_eq!(generation.job().parent_tip_display(), "32".repeat(32));
    assert_eq!(supervisor.journal.generation_count().unwrap(), 1);
    let creates = child_rpc.requests("createauxblock");
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0]["params"].as_array().unwrap().len(), 2);
    assert_eq!(
        creates[1]["params"].as_array().unwrap().len(),
        1,
        "demand keeps the ordinary RPC contract"
    );
    assert_eq!(template_rpc.requests("getblocktemplate").len(), 1);
    assert_eq!(validator_rpc.requests("getblocktemplate").len(), 1);
    generation.retire_generation().unwrap();
    assert_eq!(
        child_rpc.requests("retireauxblock").len(),
        1,
        "only the published demanded candidate has a lease"
    );
    gate.release();
}

#[test]
fn speculative_pending_cannot_make_the_same_cold_required_key_ready() {
    let directory = tempdir().unwrap();
    let gate = SpeculativeWaitGate::new();
    let (child_rpc, child_tip, held) =
        speculative_wait_fixture(&gate, Arc::new(AtomicBool::new(false)));
    let (template_rpc, validator_rpc, _) =
        parent_retry_rpcs(vec![ParentRetryAction::Accept], child_tip);
    let supervisor =
        parent_retry_supervisor(directory.path(), &child_rpc, &template_rpc, &validator_rpc);
    assert!(matches!(
        supervisor.prepare_child_candidate_timed(&mut NativePreparationTiming::new()),
        Err(MinerError::RpcError {
            code: Some(-32001),
            ..
        })
    ));
    let preparing = Arc::clone(&supervisor);
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        result_tx
            .send(preparing.prepare_generation_timed(&mut NativePreparationTiming::new()))
            .unwrap();
    });
    held.recv_timeout(Duration::from_secs(1)).unwrap();
    let before_release = result_rx.recv_timeout(Duration::from_millis(100));
    gate.release();
    worker.join().unwrap();
    let mut generation = match before_release {
        Ok(result) => {
            if let Ok(mut generation) = result {
                generation.retire_generation().unwrap();
            }
            panic!("a pending reply must not make the same cold key ready");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => result_rx.try_recv().unwrap().unwrap(),
        Err(error) => {
            panic!("required-key fixture disconnected: {error}");
        }
    };
    assert_eq!(supervisor.journal.generation_count().unwrap(), 1);
    let creates = child_rpc.requests("createauxblock");
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0]["params"][1], json!({"mode":"speculative"}));
    assert_eq!(creates[1]["params"].as_array().unwrap().len(), 1);
    generation.retire_generation().unwrap();
    assert_eq!(child_rpc.requests("retireauxblock").len(), 1);
}
