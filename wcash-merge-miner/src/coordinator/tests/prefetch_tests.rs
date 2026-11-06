use super::*;

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
    let (child_rpc, _) = prefetch_child_rpc([0x71; 20], 0, Duration::from_millis(25));
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
    assert!(child.ready_age().unwrap() >= Duration::from_millis(25));
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
